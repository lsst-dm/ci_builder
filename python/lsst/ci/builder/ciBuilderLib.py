from __future__ import annotations

__all__ = ("CommandRunner", "BaseCommand", "CommandError", "BuildState")

from abc import ABC, abstractmethod

from argparse import ArgumentParser, Namespace
from bisect import bisect_left
from dataclasses import dataclass
from functools import total_ordering
import os
import subprocess
import sys
from typing import Iterable, Optional, Tuple, Type

from lsst.log import Log
from lsst.sconsUtils.utils import libraryLoaderEnvironment


_log: Log = Log.getLogger("lsst.ci.builder.CommandRunner")


class CommandError(Exception):
    pass


@dataclass
class BuildState:
    """Instances of this object represent the current state of the RunDir.

    The current attribute is the most recently completed command.

    The attribute named dirty is a boolean that indicates if there are changes
    to the filesystem that have not been recorded into a tag of a completed
    command. A value of True means a command was run, but failed and left
    changes to the filesystem.

    Under default options, the RunDir will be reset to the last completed
    command prior to running the next command. This means that the dirty
    attribute will be `False`. However, the user may opt to run in a
    dirty state. An instance of this object will be passed to the run
    method of each command that is to be run, so they may decide if they
    can successfully run in a dirty state.
    """
    current: str
    dirty: bool


@total_ordering
@dataclass
class RegisteredCommand:
    """This is an object intended for internal use. It tracks various
    information associated with a registered command. These objects
    are sortable as to maintain a run order.
    """
    git_tag: str
    """Name to use for both the command name, which will also be used
    as the tag name in the internal git repo than manages the RunDir
    filesystem.
    """
    command: Type[BaseCommand]
    """The command class that is to be executed
    """
    run_number: float
    """Number that defines where this command will appear in the ordering of
    commands that will be run.
    """

    def __eq__(self, other):
        return self.run_number == other.run_number

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.run_number < other.run_number


class CommandRunner:
    """This is responsible for running various commands and recording the state
    of the filesystem after each command.

    Parameters
    ----------
    pkgRoot : `str`
        This is the location of the package in which the `CommandRunner` will
        execute. Run directories will be relative to this path
    """

    parser: ArgumentParser
    """Argument parser used when interpreting command line arguments
    """

    def __init__(self, pkgRoot: str):
        _log.info("Setting up CommandRunner instance")

        self.pkgRoot = pkgRoot

        self._allTags: set[str] = set()
        self.ordering: list[RegisteredCommand] = []

    def _init_ArgParse(self) -> None:
        """Initialize the command line arguments for the runner
        """
        self.parser = ArgumentParser(prog=self.__class__.__name__)
        self.parser.add_argument("command", help=("The command to run, will build all previous commands if"
                                                  " they have not already been run"),
                                 default=None, nargs='?')
        self.parser.add_argument("--list",
                                 help="List the available commands to run, sorted by order",
                                 action='store_true', default=False)
        self.parser.add_argument("--status", action='store_true', default=False,
                                 help="Print the name of the last successfully run command")
        self.parser.add_argument("--allow-dirty", help="Don't reset the state before running the specified "
                                 "command, ",
                                 action='store_true')
        self.parser.add_argument("--clean", help="Reset to the initial state before commands were run",
                                 action='store_true')
        self.parser.add_argument("--repo-root", dest="root", default=os.path.join(self.pkgRoot, "DATA"),
                                 help="Path to root of the data repository.")
        self.parser.add_argument("--enable-profile",
                                 help=("Profile base filename; "
                                       "output will be <basename>-<sequence#>-<script>.pstats; "
                                       "(Note: this option is for profiling the scripts, while "
                                       "--profile is for scons)"),
                                 nargs="?", const="profile", dest="enable_profile", type=str)
        self.parser.add_argument("-j", dest="num_cores", help="Number of cores to use for commands that "
                                 "support multiple cores", default=1, type=int)
        self.parser.add_argument("--reset", dest="reset_target", help="Reset the RunDir to given commmand",
                                 default="", type=str)
        self.addArgs()
        for regCommand in self.ordering:
            regCommand.command.addArgs(self.parser)

    def addArgs(self) -> None:
        """This is a point where additional arguments can be added in
        subclasses. The argument parser to modify will be available
        through the self.parser attribute.
        """
        return

    def _getProfiling(self, script):
        """Return python command-line argument string for profiling
        If activated (via the "--enable-profile" command-line argument),
        we write the profile to a filename starting with the provided
        base name and including a sequence number and the script name,
        so its contents can be quickly identified.
        Note that this is python function-level profiling, which won't
        descend into C++ elements of the codebase.
        A basic profile can be printed using python:
            >>> from pstats import Stats
            >>> stats = Stats("profile-123-script.pstats")
            >>> stats.sort_stats("cumulative").print_stats(30)
        """
        base = self.args.enable_profile
        if not base:
            return ""
        global profileNum
        profileNum += 1
        if script.endswith(".py"):
            script = script[:script.rfind(".")]
        return f" -m cProfile -o {base}-{profileNum:03}-{script}.pstats"

    def getExecutableCmd(self, package: str, script, args, directory=None):
        """
        Given the name of a package and a script or other executable which lies
        within the given subdirectory (defaults to "bin"), return an
        appropriate iterable which can be used to set up an appropriate
        environment and execute the command.

        This includes:
        * Specifying an explict list of paths to be searched by the dynamic
          linker;
        * Specifying a Python executable to be run (we assume the one on the
          default ${PATH} is appropriate);
        * Specifying the complete path to the script.

        Parameters
        ----------
        package : `str`
            name of the package in which command is located
        script : `str`
            name of the script to run
        args : `Iterable` of `str`
            an iterable of strings to use as arguments to the command
        directory : `str` Optional
            an optional string to specify a path to the command. If
            argument is None, a default of bin is used.

        Returns
        -------
        command : `Iterable` of `str`
            an iterable of strings that can be used as an argument
            to processes like `subprocess.run`

        """
        if directory is None:
            directory = "bin"
        cmds = [libraryLoaderEnvironment(), "python", self._getProfiling(script),
                os.path.join(os.environ[package], directory, script)]
        cmds.extend(args)
        return [c for c in cmds if c]

    def _runAndTrap(self, command: Tuple[str, ...], msg: str = "") -> subprocess.CompletedProcess[bytes]:
        """Run a git command involved with recording or restoring the
        state of the RunDir.
        """
        _log.debug(f"Running command {command}")
        commandResult = subprocess.run(self.gitCmd+command, capture_output=True)
        if (commandResult.returncode != 0
                and "nothing to commit, working tree clean" not in commandResult.stdout.decode()):
            print(commandResult.stderr)
            _log.error(f"There was an issue running the command {commandResult.stderr.decode()}")
            raise CommandError(msg.format(commandResult.stderr))
        return commandResult

    def _init_RunDir(self):
        self.RunDir = self.args.root
        self.gitCmd = ("git", "-C", self.RunDir, "-c", "user.email='\\<\\>'", "-c", "user.name=ci_builder")
        if not os.path.exists(self.RunDir):
            os.mkdir(self.RunDir)
            self.RunDir = os.path.abspath(self.RunDir)
            self._runAndTrap(('init',))
            self._runAndTrap(('commit', "--allow-empty", '-m', 'initialize'))
            self._runAndTrap(('tag', '-a', 'init', '-m', 'initial tag'))
        else:
            # Try to fetch the repo state, if this raises a command error, then
            # the git filesystem was not properly initialized
            try:
                # init the git repo if it hasn't already (e.g. empty folder)
                if not os.path.exists(os.path.join(self.RunDir, '.git')):
                    self._runAndTrap(('init',))
                self.getRepoState()
            except CommandError as err:
                # The filesystem has no tags to describe, and was not
                # initialized,
                if "describe" in err.args[0]:
                    self._runAndTrap(('commit', "--allow-empty", '-m', 'initialize'))
                    self._runAndTrap(('tag', '-a', 'init', '-m', 'initial tag'))
            self.RunDir = os.path.abspath(self.RunDir)

    def getRepoState(self) -> BuildState:
        """returns the current latest label and if the state is dirty
        """
        currentTagResult = self._runAndTrap(("describe", "--exact-match", "HEAD"),
                                            "There was an issue getting the current tag: {}")

        currentState = self._runAndTrap(('status', '-s'),
                                        "There was an issue getting the current tag: {}")
        return BuildState(currentTagResult.stdout.decode().replace('\n', ''), bool(currentState.stdout))

    def _getAllTags(self) -> Iterable[str]:
        """Returns all the tags that have been declared in the RunDir
        """
        allTagsResult = self._runAndTrap(('tag',), "Could not determine build tags {}")
        # last element is an empty string, splitting on newline
        return allTagsResult.stdout.decode().split('\n')[:-1]

    def _clean(self):
        """Resets the RunDir to a state before any commands were executed
        """
        self._runAndTrap(("reset", "--hard", "init"), "There was an issue cleaning the state: {}")
        allTags = set(self._getAllTags())
        allTags.remove("init")
        self._runAndTrap(("tag", "-d") + tuple(allTags), "There was an issue cleaning the state: {}")
        sys.exit(0)

    def _reset(self):
        """Resets a RunDir to a given state
        """
        target = self.args.reset_target
        if target not in self._allTags:
            print(f"{target} is not a vailid command to reset to")
            sys.exit(1)
        elif target not in set(self._getAllTags()):
            print(f"{target} has not been run yet")
            sys.exit(1)
        else:
            self._runAndTrap(("reset", "--hard", target), "There was an issue resetting to desired tag: {}")
            self._runAndTrap(('clean', '-dfx'),
                             "There was an issue resetting to a desired tag: {}")
            tags = [command.git_tag for command in self.ordering]
            target_index = tags.index(target)
            if target_index + 1 <= len(tags):
                extra_tags = tuple(tags[target_index+1:])
                self._runAndTrap(("tag", "-d") + extra_tags, "There was an issue clearing unneeded command"
                                 "tags")
            _log.info(f"RunDir reset to tag {target}")
            sys.exit(1)

    def _list_commands(self):
        """Prints all the commands available to run out to the console and
        exits.
        """
        for command in self.ordering:
            print(command.git_tag)
        sys.exit(0)

    def _print_status(self):
        state = self.getRepoState()
        print(f"The last command to complete is {state.current}")
        if state.dirty:
            print("The run directory is dirty, a command was run but did not complete successfully")
        sys.exit(0)

    def _buildCommandsList(self, args: Namespace) -> Tuple[list[RegisteredCommand], str]:
        """Determine what commands are to be run given the current state of the
        repository and the arguments passed on the command line. The function
        returns this list of commands and the name of the command immediately
        preceding the first command in the list. This name is used as a reset
        target to reset the state of the RunDir prior to executing the
        first command.
        """
        if args.command is None:
            # no command is specified, assume this means run starting from
            # whatever the current state of the repository is (state meaning
            # last run command) and run to the end
            state = self.getRepoState()
            # init is a special state, meaning no commands have been run
            currentIndex = -1 if state.current == 'init' else self.orderMap[state.current]
            commands = self.ordering[currentIndex+1:]
            resetTarget = state.current
        else:
            # process the commands specified in the arguments
            if args.command not in self._allTags:
                raise CommandError(f"{args.command} is not a valid command")
            command: str = args.command
            commandIndex = self.orderMap[command]

            # find all the commands that need to be run prior to the one
            # specified and subtract off all the commands that are already
            # finished in the run dir
            dependencies = (x.git_tag for x in self.ordering[:commandIndex])
            allTags = set(self._getAllTags())
            remaining = set(dependencies) - allTags

            # Find what command state must be reset to prior to running the
            # specified command
            if remaining:
                start = min([x for x in remaining], key=lambda y: self.orderMap[y])
            else:
                start = command
            startIndex = self.orderMap[start]
            if startIndex > 1:
                resetTarget = self.ordering[startIndex-1].git_tag
            else:
                resetTarget = 'init'
            commandIndex = self.orderMap[command]
            commands = self.ordering[startIndex:commandIndex+1]
        return commands, resetTarget

    def run(self, arguments: Optional[Namespace] = None) -> None:
        """Executes the commands defined by this runner class given options
        supplied on the command line, or the optional ``arguments`` function
        argument.

        Parameters
        ----------
        arguments : `~argparse.Namespace` Optional
            This may be any object that quacks like an `~argparse.Namespace`
            object and has a hierarchy that corresponds to one that would
            be produced by running `argparse.ArgumentParser.parse_args` with
            the options of the runner class.

        Raises
        ------
        CommandError
            Raised if there is any problem running one of the specified
            commands.
        """
        # create a lookup map from git_tag to position in ordered command list
        self.orderMap = {v.git_tag: i for i, v in enumerate(self.ordering)}

        # initialize the argument parser
        self._init_ArgParse()
        # parse the arguments, or use the supplied Namespace
        if arguments is None:
            try:
                cmdArgs = sys.argv[1:]
                args = self.parser.parse_args(cmdArgs)
            except Exception:
                self.parser.print_help()
                sys.exit(0)
        else:
            args = arguments

        self.args = args

        # list all commands (exits on completion)
        if args.list:
            self._list_commands()

        # initialize the run dir
        self._init_RunDir()

        # if status is requested print last completed command and the state
        # of the filesystem (exits on completion). It only makes sense for
        # this command to work after RunDir has been init
        if args.status:
            self._print_status()

        # Run clean if requested (exits on completion) must be run after
        # RunDir init
        if args.clean:
            self._clean()

        # Reset RunDir to given command (exits on completion)
        if args.reset_target != "":
            self._reset()

        # get the commands that need to be run, and what state to reset
        # the RunDir to prior to executing it
        commands, resetTarget = self._buildCommandsList(args)

        if not commands:
            _log.info("All commands have been run")
            sys.exit(0)

        if not args.allow_dirty:
            _log.debug(f"Reseting RunDir to previous state of {resetTarget}")
            self._runAndTrap(('reset', '--hard', resetTarget),
                             "There was an issue resetting to a clean state")
            self._runAndTrap(('clean', '-dfx'),
                             "There was an issue resetting to a clean state")

        # delete any tags past the one the first one that will be processed
        allTags = set(self._getAllTags())

        firstCommandIndex = self.orderMap[commands[0].git_tag]
        tagsToDelete = set((x.git_tag for x in self.ordering[firstCommandIndex:])) & allTags
        _log.debug(f"Removing tags: {tagsToDelete}")
        self._runAndTrap(("tag", "-d") + tuple(tagsToDelete), "There was an issue deleting old tags {}")

        for regCommand in commands:
            try:
                # Reset to the run directory in case a command changed
                # locations
                os.chdir(self.RunDir)
                _log.info(f"Running command: {regCommand.git_tag}")
                cmd = regCommand.command(args, self)
                cmd.run(self.getRepoState())
            except CommandError:
                # This is in place for future work, but for now re-raise the
                # error
                raise
            self._runAndTrap(("add", "."), f"There was an issue adding command {cmd} output: {{}}")
            self._runAndTrap(("commit", "-m", f"{regCommand.git_tag} completed"),
                             f"There was an committing the command {cmd} output: {{}}")
            self._runAndTrap(("tag", '-a', regCommand.git_tag, '-m', '', '-f'),
                             f"There was an issue tagging command {cmd}: {{}}")

    def register(self, gitTag: str, runNumber: float):
        r"""This method is a decorator for `BaseCommand`\ s which registers
        those commands with this instance of a `CommandRunner`.

        Parameters
        ----------
        gitTag : str
            A string that will be used to both identify the decorated command
            inside the `CommandRunner`, and will be used as a git tag when
            checkpointing the filesystem state
        runNumber : float
            A floating point number that is used to order this command with
            the list of commands that the `CommandRunner` could execute
        """
        def wrapped(command: Type[BaseCommand]):
            if gitTag in self._allTags:
                raise ValueError("Each command must have a unique GitTag")

            self._allTags.add(gitTag)
            regCommand = RegisteredCommand(gitTag, command, runNumber)
            pos = bisect_left(self.ordering, regCommand)
            self.ordering.insert(pos, regCommand)
        return wrapped


class BaseCommand(ABC):
    """This is an abstract base class used to specify the interface that
    runnable commands must adhere to. Subclasses must override a run method.
    This method should run any commands that modify the state a managed
    repository.

    Commands by themselves are not associate with any particular
    `CommandRunner` through construction. This allows individual commands to be
    used in multiple independent `CommandRunner` instances.

    To associate a `BaseCommand` subclass with `CommandRunner` instance, use
    the `CommandRunner` `register` method.

    `BaseCommand` supports an optional classmethod named `addArgs`. This method
    is given the argument parser object from a `CommandRunner` instance, and is
    free to add or modify command line argument options. This allows individual
    Commands to request additional options or information from the user that
    may be needed to execute a command.
    """
    def __init__(self, arguments: Namespace, runner: CommandRunner):
        self.arguments = arguments
        self.runner = runner

    def __init_subclass__(cls, **kwargs):
        for base in cls.__bases__:
            for name in base.__dict__:
                if hasattr(cls, name):
                    value = getattr(cls, name)
                    if value is NotImplemented:
                        raise NotImplementedError(f"Class attribute `{name}` must be overloaded in "
                                                  "subclasses")
        super().__init_subclass__(**kwargs)

    @classmethod
    def addArgs(cls, parser: ArgumentParser):
        pass

    @abstractmethod
    def run(self, currentState: BuildState):
        raise NotImplementedError("Override this in a subclass")
