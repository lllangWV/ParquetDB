import inspect
import logging
import os
import sys
from typing import Callable, Dict, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from parquetdb import ParquetDB
from parquetdb.core import types
from parquetdb.utils.log_utils import set_verbose_level

logger = logging.getLogger(__name__)


def validate_generator_inputs(args):
    from parquetdb.graph.edges import EdgeStore
    from parquetdb.graph.nodes import NodeStore

    ALLOWED_GENERATOR_INPUTS = (EdgeStore, NodeStore)

    for arg in args:
        if not isinstance(arg, ALLOWED_GENERATOR_INPUTS):
            raise ValueError(
                f"Generator input must be a {ALLOWED_GENERATOR_INPUTS}, {arg} is {type(arg)}"
            )


class GeneratorStore(ParquetDB):
    """
    A store for managing generator functions in a graph database.
    This class handles serialization, storage, and loading of functions
    that generate edges between nodes.
    """

    required_fields = ["generator_name", "generator_func"]

    def __init__(
        self,
        storage_path: str,
        initial_fields: List[pa.Field] = None,
        verbose: int = 1,
    ):
        """
        Initialize the EdgeGeneratorStore.

        Parameters
        ----------
        storage_path : str
            Path where the generator functions will be stored

        """

        if initial_fields is None:
            initial_fields = []

        initial_fields.extend(
            [
                pa.field("generator_name", pa.string()),
                pa.field("generator_func", types.PythonObjectArrowType()),
            ]
        )
        super().__init__(
            db_path=storage_path,
            initial_fields=initial_fields,
            serialize_python_objects=True,
            verbose=verbose,
        )

        self._initialize_metadata()
        logger.debug(f"Initialized GeneratorStore at {storage_path}")

    def __repr__(self):
        return self.summary(show_column_names=True)

    def _initialize_metadata(self):
        pass

    #     """Initialize store metadata if not present."""
    #     metadata = self.get_metadata()
    #     update_metadata = False
    #     for key in self.metadata_keys:
    #         if key not in metadata:
    #             update_metadata = True
    #             break

    #     if update_metadata:
    #         self.set_metadata(
    #             {
    #                 "
    #             }
    #         )

    @property
    def storage_path(self):
        return self._db_path

    @property
    def n_generators(self):
        return self.read(columns=["generator_name"]).num_rows

    @property
    def generator_names(self):
        return (
            self.read(columns=["generator_name"]).to_pandas()["generator_name"].tolist()
        )

    def summary(self, show_column_names: bool = False):
        fields_metadata = self.get_field_metadata()
        metadata = self.get_metadata()

        # Header section
        tmp_str = f"{'=' * 60}\n"
        tmp_str += f"GENERATOR STORE SUMMARY\n"
        tmp_str += f"{'=' * 60}\n"
        tmp_str += f"• Number of generators: {self.n_generators}\n"
        tmp_str += f"Storage path: {os.path.relpath(self.storage_path)}\n\n"

        # Metadata section
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"METADATA\n"
        tmp_str += f"{'#' * 60}\n"
        for key, value in metadata.items():
            tmp_str += f"• {key}: {value}\n"

        # Generator details
        tmp_str += f"\n{'#' * 60}\n"
        tmp_str += f"GENERATOR DETAILS\n"
        tmp_str += f"{'#' * 60}\n"
        if show_column_names:
            tmp_str += f"• Columns:\n"
            for col in self.get_schema().names:
                tmp_str += f"    - {col}\n"

                if fields_metadata[col]:
                    tmp_str += f"       - Field metadata\n"
                    for key, value in fields_metadata[col].items():
                        tmp_str += f"           - {key}: {value}\n"

            # Show generator names
            tmp_str += f"\n• Generator names:\n"
            for name in self.generator_names:
                tmp_str += f"    - {name}\n"

        return tmp_str

    def store_generator(
        self,
        generator_func: Callable,
        generator_name: str,
        generator_args: Dict = None,
        generator_kwargs: Dict = None,
        create_kwargs: Dict = None,
    ) -> None:
        """
        Store an edge generator function.

        Parameters
        ----------
        generator_func : Callable
            The function that generates edges
        generator_name : str
            Name to identify the generator function
        generator_args : Dict
            Arguments to pass to the generator function
        generator_kwargs : Dict
            Keyword arguments to pass to the generator function
        create_kwargs : Dict
            Keyword arguments to pass to the create method
        """
        if create_kwargs is None:
            create_kwargs = {}
        if generator_args is None:
            generator_args = {}
        if generator_kwargs is None:
            generator_kwargs = get_function_kwargs(generator_func)
        try:
            df = self.read(columns=["generator_name"]).to_pandas()

            if generator_name in df["generator_name"].values:
                logger.warning(f"Generator '{generator_name}' already exists")
                return None

            # Serialize the function using dill

            # Create data record
            extra_fields = {}
            for key, value in generator_args.items():
                extra_fields[f"generator_args.{key}"] = value

            for key, value in generator_kwargs.items():
                extra_fields[f"generator_kwargs.{key}"] = value

            data = [
                {
                    "generator_name": generator_name,
                    "generator_func": generator_func,
                    **extra_fields,
                }
            ]
            # Store the function data
            self.create(data=data, **create_kwargs)
            logger.info(f"Successfully stored generator '{generator_name}'")

        except Exception as e:
            logger.error(f"Failed to store generator '{generator_name}': {str(e)}")
            raise

    def load_generator_data(self, generator_name: str) -> pd.DataFrame:
        filters = [pc.field("generator_name") == generator_name]
        table = self.read(filters=filters)

        for column_name in table.column_names:
            logger.debug(f"Loading generator data for column: {column_name}")
            col_array = table[column_name].drop_null()
            if len(col_array) == 0:
                table = table.drop(column_name)

        if len(table) == 0:
            raise ValueError(f"No generator found with name '{generator_name}'")
        return table.to_pandas()

    def is_in(self, generator_name: str) -> bool:
        filters = [pc.field("generator_name") == generator_name]
        table = self.read(filters=filters)
        return len(table) > 0

    def load_generator(self, generator_name: str) -> Callable:
        """
        Load an edge generator function by name.

        Parameters
        ----------
        generator_name : str
            Name of the generator function to load

        Returns
        -------
        Callable
            The loaded generator function
        """
        try:
            df = self.load_generator_data(generator_name)
            generator_func = df["generator_func"].iloc[0]
            return generator_func

        except Exception as e:
            logger.error(f"Failed to load generator '{generator_name}': {str(e)}")
            raise

    def list_generators(self) -> List[Dict]:
        """
        List all stored edge generators.

        Returns
        -------
        List[Dict]
            List of dictionaries containing generator information
        """
        try:
            result = self.read(columns=["generator_name"])
            return result.to_pylist()
        except Exception as e:
            logger.error(f"Failed to list generators: {str(e)}")
            raise

    def delete_generator(self, generator_name: str) -> None:
        """
        Delete a generator by name.

        Parameters
        ----------
        generator_name : str
            Name of the generator to delete
        """
        try:
            filters = [pc.field("generator_name") == generator_name]
            self.delete(filters=filters)
            logger.info(f"Successfully deleted generator '{generator_name}'")
        except Exception as e:
            logger.error(f"Failed to delete generator '{generator_name}': {str(e)}")
            raise

    def run_generator(
        self,
        generator_name: str,
        generator_args: Dict = None,
        generator_kwargs: Dict = None,
    ) -> None:
        """
        Run a generator function by name.
        """

        if generator_args is None:
            generator_args = {}
        if generator_kwargs is None:
            generator_kwargs = {}

        df = self.load_generator_data(generator_name)
        for column_name in df.columns:
            value = df[column_name].iloc[0]
            if "generator_args" in column_name:
                arg_name = column_name.split(".")[-1]

                # Do not overwrite user-provided args
                if arg_name not in generator_args:

                    generator_args[arg_name] = value
            elif "generator_kwargs" in column_name and value is not None:
                kwarg_name = column_name.split(".")[-1]

                # Do not overwrite user-provided kwargs
                if kwarg_name not in generator_kwargs:

                    generator_kwargs[kwarg_name] = value

        generator_func = df["generator_func"].iloc[0]

        logger.debug(f"Generator func: {generator_func}")
        logger.debug(f"Generator args: {generator_args}")
        logger.debug(f"Generator kwargs: {generator_kwargs}")

        arg_names = get_function_arg_names(generator_func)
        generator_args = [generator_args[k] for k in arg_names]

        logger.debug(f"Running {generator_func.__name__} with args: {generator_args}")
        logger.debug(
            f"Running {generator_func.__name__} with kwargs: {generator_kwargs}"
        )
        return generator_func(*generator_args, **generator_kwargs)


def get_function_arg_names(func):
    sig = inspect.signature(func)
    return [
        name
        for name, param in sig.parameters.items()
        if param.default == inspect.Parameter.empty
    ]


def get_function_kwargs(func):
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if param.default != inspect.Parameter.empty
    }
