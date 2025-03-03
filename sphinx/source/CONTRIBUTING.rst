Contributing
==================================

We welcome contributions and we hope that this guide will
facilitate an understanding of the ParquetDB code repository. It is
important to note that the ParquetDB software package is maintained on a
volunteer basis and thus we need to foster a community that can support
user questions and develop new features to make this software a useful
tool for all users.

This page is dedicated to outline where you should start with your
question, concern, feature request, or desire to contribute.

Being Respectful
-----------------------------------

Please demonstrate empathy and kindness toward other people, other software,
and the communities who have worked diligently to build (un-)related tools.

Please do not talk down in Pull Requests, Issues, or otherwise in a way that
portrays other people or their works in a negative light.

Cloning the Source Repository
-----------------------------------

You can clone the source repository from
`<https://github.com/lllangWV/ParquetDB>`_ and install the latest version by
running:

.. code:: bash

   git clone https://github.com/lllangWV/ParquetDB.git
   cd ParquetDB
   
Next, create a virtual envrionment and activate it. 

.. code:: bash

   conda create -n parquetdb python=3.10
   conda activate parquetdb
   pip install -e .[docs]

Change to the dev branch to add changes

.. code:: bash

   git checkout dev
   
Updating documentation
-----------------------------------

The documentation for parquetdb are generated by using sphinx and the sphinx-gallery packages. 
If you add code to the package, make sure to add the proper doc strings to be automatically generated.

To generate the documentation you will need to run the following code from the top-level directory:

.. code:: bash

   cd sphinx
   make clean & make html

This will clean the sphinx/_build directory and it will remove all aut-generated docs.
Once make html is called it will start generating the html files and store them in sphinx/_build.
After you have check the documentation and make sure there are no warnings or errors,
you will need to copy the contents of sphinx/_build/html/ to docs and save over 
everything in that directory. This can be achieved by running the below code:

.. code:: bash

   make deploy


Finally, you can push the changes to github.

Running tests 
-----------------------------------

In the current version of parquetdb, we have added to external tests to test the functionality of the package. 
To do this you will need to download the development data by running the following code.

.. code:: python

   python tests/test_parquetdb.py
   python tests/test_types.py

This will download the development data to parquetdb/data/examples.

Now to run the tests, from the top directory run pytest

