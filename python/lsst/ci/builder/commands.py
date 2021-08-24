from argparse import ArgumentParser
import os
import subprocess

from typing import Iterable, Union

from lsst.daf.butler.script import createRepo, butlerImport
from lsst.obs.base.script import (registerInstrument, writeCuratedCalibrations, ingestRaws, defineVisits)
from lsst.pipe.tasks.script import registerSkymap

from lsst.ci.builder import BaseCommand, BuildState


class CreateButler(BaseCommand):
    @classmethod
    def addArgs(cls, parser: ArgumentParser):
        parser.add_argument("--butler-config", dest="butler_conf", default="",
                            help="Path to an external Butler config used to create a data repository.")
        parser.add_argument("--config-override", action="store_true", dest="conf_override",
                            help="Override the default config root with the given repo-root.")

    def run(self, currentState: BuildState):
        conf = self.arguments.butler_conf

        createRepo(self.runner.RunDir, seed_config=conf or None, override=self.arguments.conf_override)


class RegisterInstrument(BaseCommand):
    instrumentName: Union[str, Iterable[str]] = NotImplemented
    """Qualified class name (or list of names) of the instrument, must be
    overloaded in subclass.
    """

    def run(self, currentState: BuildState):
        if isinstance(instrument := self.instrumentName, str):
            instrument = (instrument,)
        registerInstrument(self.runner.RunDir, instrument)


class WriteCuratedCalibrations(BaseCommand):
    instrumentName: str = NotImplemented
    """This must be overloaded with the qualified class name, or the Instrument
    name.
    """

    def run(self, currentState: BuildState):
        writeCuratedCalibrations(self.runner.RunDir, self.instrumentName, None, tuple())


class RegisterSkyMap(BaseCommand):
    relativeConfigPath: str = os.path.join("configs", "skymap.py")

    @classmethod
    def addArgs(cls, parser: ArgumentParser):
        parser.add_argument("--skymap-config", dest="skymap_config", default="",
                            help="Path to a config file to used when registering a SkyMap")

    def run(self, currentState: BuildState):
        if not (config_file := self.arguments.skymap_config):
            config_path = os.path.join(self.runner.pkgRoot, self.relativeConfigPath)
            if os.path.exists(config_path):
                config_file = config_path
        registerSkymap.registerSkymap(self.runner.RunDir, None, config_file or None)


class IngestRaws(BaseCommand):
    FITS_RE: str = r"\.fit[s]?\b"
    rawLocation: str = NotImplemented
    """"This must be overloaded with the path to raw data"
    """

    def run(self, currentState: BuildState):
        ingestRaws(self.runner.RunDir, (self.rawLocation,), self.FITS_RE, None,
                   processes=int(self.arguments.num_cores))


class DefineVisits(BaseCommand):
    instrumentName: str = NotImplemented
    """"This must be overloaded with the qualified class name, or the
    Instrument name
    """
    collectionsName: str = NotImplemented
    """This must be overloaded with the collection name declared visits
    """

    def run(self, currentState: BuildState):
        defineVisits(self.runner.RunDir, None, self.collectionsName, self.instrumentName,
                     processes=int(self.arguments.num_cores))


class ButlerImport(BaseCommand):
    dataLocation: str = NotImplemented
    """This must be overloaded with the path to data for import
    """
    importFileLocation: str = NotImplemented
    """This must be overloaded with the path to import file location
    """

    def run(self, currentState: BuildState):
        butlerImport(self.runner.RunDir, self.dataLocation, self.importFileLocation, 'auto', None, False)


class TestRunner(BaseCommand):
    """Replace this with some PyTest stuff in the future"""
    @property
    def testLocation(self) -> str:
        """This may be overloaded with test location"""
        return os.path.join(self.runner.pkgRoot, "tests")

    @property
    def executable(self) -> str:
        return os.path.join(self.runner.pkgRoot, "bin", "sip_safe_python.sh")

    def run(self, currentState: BuildState):
        testLoc = self.testLocation
        for file in os.listdir(testLoc):
            test = os.path.join(testLoc, file)
        if test.endswith(".py"):
            subprocess.run((self.executable, test), check=True)
