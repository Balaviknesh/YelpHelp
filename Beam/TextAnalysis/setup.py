from setuptools import setup
from setuptools.command.install import install as _install


class Install(_install):
    def run(self):
        _install.do_egg_install(self)
        import nltk
        nltk.download("popular")


setup(
    cmdclass={'install': Install},
    install_requires=['nltk'],
    setup_requires=['nltk'])
