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

# __version_info__ format:
# (major, minor, patch, dev/alpha/beta/rc/final, #)

# (1, 2, 1, 'dev', 21) => "1.2.1.dev21"
# (1, 2, 1, 'alpha', 7) => "1.2.1a7"
# (1, 2, 1, 'beta', 2) => "1.2.1b2"
# (1, 2, 1, 'rc', 4) => "1.2.1rc4"
# (1, 2, 1, 'final', 0) => "1.2.1"

# (1, 2, 0, 'dev', 21) => "1.2.dev21"
# (1, 2, 0, 'alpha', 7) => "1.2a7"
# (1, 2, 0, 'beta', 2) => "1.2b2"
# (1, 2, 0, 'rc', 4) => "1.2rc4"
# (1, 2, 0, 'final', 0) => "1.2"


__version_info__ = (0, 1, 114, "post", 3)

assert len(__version_info__) == 5
assert __version_info__[3] in ('dev', 'alpha', 'beta', 'rc', 'final', 'post')

__version__ = '.'.join(map(str, __version_info__[:2 if __version_info__[2] == 0 else 3]))

if __version_info__[3] in ('dev', 'post'):
    __version__ += '.' + __version_info__[3] + str(__version_info__[4])
elif __version_info__[3] != 'final':
    __version__ += {'alpha': 'a', 'beta': 'b', 'rc': 'rc'}[__version_info__[3]] + str(__version_info__[4])
