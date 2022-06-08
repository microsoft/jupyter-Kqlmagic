..
  # -------------------------------------------------------------------------
  # Copyright (c) Microsoft Corporation. All rights reserved.
  # Licensed under the MIT License. See License.txt in the project root for
  # license information.
  # --------------------------------------------------------------------------*)

A Jupyter kernel for kqlmagic
=============================


Prerequisites
-------------
`Jupyter Notebook <https://docs.jupyter.org/en/latest/index.html>`_ and 
`Kqlmagic <https://github.com/microsoft/jupyter-Kqlmagic/blob/master/README.md>`_

Installation
------------
To install using pip::

    pip install kqlmagic_kernel

Add ``--user`` to install in the user-level environment instead of the system environment.

To install using conda::

    conda config --add channels conda-forge
    conda install kqlmagic_kernel
    conda install texinfo # For the inline documentation (shift-tab) to appear.
    python -m kqlmagic_kernel.install

Kqlmagic Jupyter kernelspec is automatically installed as part of the
python package.  This location can be found using ``jupyter kernelspec list``.
If the default location is not desired, remove the directory for the
``kqlmagic`` kernel, and install using ``python -m kqlmagic_kernel install``.  See
``python -m kqlmagic_kernel install --help`` for available options.

Usage
-----
To use the kernel, run one of:

.. code:: shell

    jupyter notebook
    # In the notebook interface, select Kqlmagic from the 'New' menu
    jupyter qtconsole --kernel kqlmagic
    jupyter console --kernel kqlmagic

This kernel is based on `ipykernel <https://github.com/ipython/ipykernel>`_,
which means it features a standard set of magics (such as ``%%html``).  For a full list of magics,
run ``%lsmagic`` in a cell.

A sample notebook is available online_.

Configuration
-------------
The kernel can be configured by adding an ``kqlmagic_kernel_config.py`` file to the
``jupyter`` config path.

The path to the Kqlmagic kernel JSON file can also be specified by creating an
``KQLMAGIC_KERNEL_JSON`` environment variable.

Troubleshooting
---------------

Kernel Times Out While Starting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If the kernel does not start, run the following command from a terminal:

.. code:: shell

    python -m kqlmagic_kernel.check

This can help diagnose problems with setting up integration with Octave.  If in doubt,
create an issue with the output of that command.


Kernel is Not Listed
~~~~~~~~~~~~~~~~~~~~
If the kernel is not listed as an available kernel, first try the following command:

.. code:: shell

    python -m kqlmagic_kernel install --user

If the kernel is still not listed, verify that the following point to the same
version of python:

.. code:: shell

    which python  # use "where" if using cmd.exe
    which jupyter

For details of how this works, see the Jupyter docs on `wrapper kernels
<http://jupyter-client.readthedocs.org/en/latest/wrapperkernels.html>`_

.. _online: https://mybinder.org/v2/gh/Microsoft/jupyter-Kqlmagic/master?filepath=notebooks%2FQuickStartLA.ipynb