# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from ipykernel.kernelapp import IPKernelApp
from .kernel import KqlmagicKernel

if __name__ == '__main__':
    IPKernelApp.launch_instance(kernel_class=KqlmagicKernel)