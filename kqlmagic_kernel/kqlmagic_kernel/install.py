# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import json
import os
import sys
import argparse


from jupyter_client.kernelspec import KernelSpecManager
from IPython.utils.tempdir import TemporaryDirectory


kernel_json = {"argv":[sys.executable, "-m", "kqlmagic_kernel", "-f", "{connection_file}"],
    "display_name":"Kqlmagic",
    "language":"kql",
    # "codemirror_mode":"shell",
    # "env":{"PS1": "$"}
}


def install_my_kernel_spec(user=True, prefix=None):
    with TemporaryDirectory() as td:
        os.chmod(td, 0o755) # Starts off as 700, not user readable
        with open(os.path.join(td, 'kernel.json'), 'w') as f:
            json.dump(kernel_json, f, sort_keys=True)
            # TODO: Copy resources once they're specified

        print('Installing IPython kernel spec')
        KernelSpecManager().install_kernel_spec(td, 'kqlmagic', user=user, replace=True, prefix=prefix)


def _is_root():
    try:
        return os.geteuid() == 0
    except AttributeError:
        return False # assume not an admin on non-Unix platforms


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Install KernelSpec for Kqlmagic Kernel'
    )

    prefix_locations = parser.add_mutually_exclusive_group()

    prefix_locations.add_argument(
        '--user',
        help="Install to the per-user kernels registry. Default if not root.",
        action='store_true'
    )
    prefix_locations.add_argument(
        '--sys-prefix',
        help="Install to sys.prefix (e.g. a virtualenv or conda env)",
        action='store_true',
        dest='sys_prefix'
    )
    prefix_locations.add_argument(
        '--prefix',
        help="Install to the given prefix. "
             "Kernelspec will be installed in {PREFIX}/share/jupyter/kernels/",
        default=None
    )

    args = parser.parse_args(argv)

    user = False
    prefix = None
    if args.sys_prefix:
        prefix = sys.prefix
    elif args.prefix:
        prefix = args.prefix
    elif args.user or not _is_root():
        user = True

    install_my_kernel_spec(user=user, prefix=prefix)


if __name__ == '__main__':
    main()