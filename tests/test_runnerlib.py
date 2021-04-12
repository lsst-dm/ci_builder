import os

from lsst.ci.builder import ciBuilderLib


inst = ciBuilderLib.CommandRunner(os.path.join(os.environ["CI_BUILDER_DIR"], 'tests'))


@inst.register(gitTag="first", runNumber=1)
class TestCommand1(ciBuilderLib.BaseCommand):
    def run(self, state):
        print("running first")
        with open("firstCommand.txt", 'w+') as f:
            f.write('first')


@inst.register(gitTag="second", runNumber=2)
class TestCommand2(ciBuilderLib.BaseCommand):
    def run(self, state):
        print("running second")
        with open("secondCommand.txt", 'w+') as f:
            f.write('second')


@inst.register(gitTag="third", runNumber=3)
class TestCommand3(ciBuilderLib.BaseCommand):
    def run(self, state):
        print("running third")
        with open("thirdCommand.txt", 'w+') as f:
            f.write('third')


if __name__ == "__main__":
    inst.run()
