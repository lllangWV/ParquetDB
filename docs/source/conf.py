# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import shutil
import sys
from distutils.sysconfig import get_python_lib
from pathlib import Path

from parquetdb._version import version

# ---------------
project = "ParquetDB"
copyright = "2024, Logan Lang"
author = "Logan Lang"


sys.path.insert(0, os.path.abspath("."))

SRC_DIR = Path(__file__).parent
REPO_ROOT = SRC_DIR.parent.parent
SRC_EXAMPLES_PATH = SRC_DIR / "examples"
REPO_EXAMPLES_PATH = REPO_ROOT / "examples"
CONTRIBUTING_PATH = REPO_ROOT / "CONTRIBUTING.md"


print(f"REPO_ROOT: {REPO_ROOT}")
print(f"SRC_DIR: {SRC_DIR}")
print(f"SRC_EXAMPLES_PATH: {SRC_EXAMPLES_PATH}")


# Copy Repo Examples to docs source directory
if SRC_EXAMPLES_PATH.exists():
    shutil.rmtree(SRC_EXAMPLES_PATH)
shutil.copytree(REPO_EXAMPLES_PATH, SRC_EXAMPLES_PATH)

shutil.copy(CONTRIBUTING_PATH, SRC_DIR / "CONTRIBUTING.md")


if os.environ.get("READTHEDOCS") == "True":

    site_path = get_python_lib()
    ffmpeg_path = os.path.join(site_path, "imageio_ffmpeg", "binaries")
    print("########")
    print("good1")
    [ffmpeg_bin] = [
        file for file in os.listdir(ffmpeg_path) if file.startswith("ffmpeg-")
    ]
    print("########*****")
    print("good2")
    try:
        os.symlink(
            os.path.join(ffmpeg_path, ffmpeg_bin), os.path.join(ffmpeg_path, "ffmpeg")
        )
    except FileExistsError:
        print("File is already there!!!!!!!")
    else:
        print("file created :)")
    print("good3")
    os.environ["PATH"] += os.pathsep + ffmpeg_path
    print("good4")

# sys.path.append(os.path.abspath("./_ext"))
# -- Project information -----------------------------------------------------


# The full version, including alpha/beta/rc tags
release = version

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx_copybutton",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "nbsphinx",
    "sphinx_new_tab_link",
    "sphinx.ext.intersphinx",
    "sphinxcontrib.youtube",
    "sphinxcontrib.video",
    "sphinx_design",
    "sphinx.ext.githubpages",
    "sphinx.ext.napoleon",
    "numpydoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "myst_parser",
    # "sphinx-nbexamples",
    # "sphinx_gallery.gen_gallery",
    # 'sphinx.youtube',
    # 'monokai_colors.ManimMonokaiStyle'
]
nbsphinx_allow_errors = True
pygments_style = "sphinx"


# sphinx_gallery_conf = {
#     # convert rst to md for ipynb
#     "pypandoc": True,
#     # path to your examples scripts
#     "examples_dirs": ["../../examples/notebooks"],
#     # path where to save gallery generated examples
#     "gallery_dirs": ["examples"],
#     "doc_module": "parquetdb",
#     "image_scrapers": ("pyvista", "matplotlib"),
# }

# example_gallery_config = {
#     "examples_dirs": ["../../examples/notebooks"],
#     "gallery_dirs": ["../../examples/notebooks"],
#     "pattern": "*.ipynb",
#     "urls": "https://github.com/lllangWV/ParquetDB/blob/main/examples/notebooks",
# }


# Configuration for the Napoleon extension
napoleon_use_ivar = True
napoleon_use_rtype = False
napoleon_use_param = False
napoleon_custom_sections = [
    ("My Custom Section", "params_style"),  # Custom section treated like parameters
    ("Plot Appearance", "params_style"),  # Custom section treated like parameters
    ("Surface Configuration", "params_style"),  # Custom section treated like parameters
    ("Spin Settings", "params_style"),  # Custom section treated like parameters
    ("Axes and Labels", "params_style"),  # Custom section treated like parameters
    (
        "Brillouin Zone Styling",
        "params_style",
    ),  # Custom section treated like parameters
    (
        "Advanced Configurations",
        "params_style",
    ),  # Custom section treated like parameters
    ("Isoslider Settings", "params_style"),  # Custom section treated like parameters
    ("Miscellaneous", "params_style"),  # Custom section treated like parameters
    "Methods",
]


numpydoc_use_plots = True
numpydoc_show_class_members = False
numpydoc_xref_param_type = True


autosummary_generate = True
add_module_names = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_title = f"ParquetDB Docs: v{version}"
html_theme = "furo"
html_logo = os.path.join("media", "images", "ParquetDB_logo.png")
html_static_path = []
# html_theme_options = {
#     # "logo": {"logo_link": "index.html"}  # Links to the root of your documentation
#     # Add any other theme-specific options here
# }
# html_favicon = '_static/flyingframes_favicon.ico'
html_context = {
    "logo_link": "index.html",  # Specify the link for the logo if needed
}

html_css_files = ["css/custom.css", "notebook.css"]

html_js_files = ["js/custom.js"]

pygments_dark_style = "monokai_colors.ManimMonokaiStyle"

if not os.path.exists("media/images"):
    os.makedirs("media/images")

if not os.path.exists("media/videos/480p30"):
    os.makedirs("media/videos/480p30")

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
