"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.ci.builder


_g = globals()
_g.update(build_package_configs(
    project_name='ci_builder',
    version=lsst.ci.builder.version.__version__))
