#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# because it is also executed from setup.py, make sure
# that it imports only modules, that for sure will exist at setup.py execution
# TRY TO KEEP IMPORTS TO MINIMUM
# --------------------------------------------------------------------------

import sys
from setuptools.command.test import test as TestCommand


class TestClass(TestCommand):

    user_options = [('pytest-args=', 'a', "Arguments to pass into py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        try:
            from multiprocessing import cpu_count
            self.pytest_args = ['-n', str(cpu_count()), '--boxed']
        except (ImportError, NotImplementedError):
            self.pytest_args = ['-n', '1', '--boxed']


    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True


    def run_tests(self):
        try:
            import pytest # type: ignore reportMissingImports # pylint: disable=import-error
        except: 
            pass
        else:
            errno = pytest.main(self.pytest_args)
            sys.exit(errno)
