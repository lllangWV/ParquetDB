from typing import Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.api.extensions import register_extension_dtype
from pandas.core.arrays.base import ExtensionArray
from pandas.core.dtypes.base import ExtensionDtype
from pandas.core.dtypes.dtypes import PandasExtensionDtype

from parquetdb.utils import data_utils, mp_utils


class PythonObjectArrowType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "parquetdb.PythonObjectArrow")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return PythonObjectArrowType()

    def __arrow_ext_class__(self):
        return PythonObjectArrowArray

    def to_pandas_dtype(self):
        return PythonObjectPandasDtype()

    def __arrow_ext_scalar_class__(self):
        return PythonObjectArrowScalar


pa.register_extension_type(PythonObjectArrowType())


class PythonObjectArrowScalar(pa.ExtensionScalar):
    def as_py(self):
        return data_utils.load_python_object(self.value.as_py())

    def equal(self, other):
        return self.value == other.value


class PythonObjectArrowArray(pa.ExtensionArray):

    def to_pandas(self, **kwargs):
        # values=self.storage.to_pandas(**kwargs).values
        values = self.storage.to_numpy(zero_copy_only=False)
        results = mp_utils.parallel_apply(data_utils.load_python_object, values)
        return pd.Series(results)

    def to_values(self, **kwargs):
        values = self.storage.to_pandas(**kwargs).values

        results = mp_utils.parallel_apply(data_utils.load_python_object, values)
        return results


@register_extension_dtype
class PythonObjectPandasDtype(PandasExtensionDtype, ExtensionDtype):
    # Some required properties
    name = "PythonObjectPandas"
    type = object
    na_value = np.nan

    @property
    def _is_boolean(self):
        return False

    def __from_arrow__(self, array: Union[pa.Array, pa.ChunkedArray]):
        """
        Given a (chunked) PyArrow array of type "python_object_dtype", convert to
        a Pandas ExtensionArray by dill-deserializing each binary blob.
        """
        return PythonObjectPandasArray(array.combine_chunks().to_values())
        # return PythonObjectPandasArray(array)

    def __eq__(self, other):
        """
        Defines equality logic. If two dtypes have the same 'name' and are of
        the same class, consider them equal.
        """
        if isinstance(other, PythonObjectPandasDtype):
            # If there are any additional parameters that define your dtype,
            # compare them here. For a simple dtype, comparing names (or classes)
            # is often enough.
            return True
        return False

    def __hash__(self):
        """
        Must return a stable integer hash. A common pattern is to hash a tuple
        of (class, name) or relevant parameters.
        """
        return hash((type(self), self.name))

    @classmethod
    def construct_array_type(cls):
        return PythonObjectPandasArray


class PythonObjectPandasArray(ExtensionArray):
    """
    A simple ExtensionArray holding arbitrary Python objects after dill.loads
    """

    _dtype = PythonObjectPandasDtype()

    def __init__(self, data):
        # store the (already deserialized) Python objects in a list
        value_array = np.empty(shape=(len(data),), dtype=object)
        value_array[:] = data
        self._data = value_array

    def __arrow_array__(self, type=PythonObjectArrowType()):
        # convert the underlying array values to a pyarrow Array
        results = mp_utils.parallel_apply(data_utils.dump_python_object, self._data)

        return pa.array(results, type=PythonObjectArrowType())

    def __array__(self, dtype=None):
        """
        Return a NumPy array representation of our data.
        Typically, we keep dtype=object so each element can be an arbitrary Python object.
        """
        if dtype is None:
            dtype = object
        # value_array=np.empty(shape=(len(self._data),1), dtype=dtype)
        # value_array[:,0]=self._data

        value_array = np.empty(shape=(len(self._data),), dtype=dtype)
        value_array[:] = self._data
        return value_array

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        """
        Construct from a sequence of Python objects. This is called, for example,
        if someone does pd.array([...], dtype="my_extension_dtype").
        """
        return cls(scalars)

    @classmethod
    def from_vectors(cls, data, dtype=None, copy=False):
        """
        Construct from a sequence of Python objects. This is called, for example,
        if someone does pd.array([...], dtype="my_extension_dtype").
        """
        return cls(data)

    @classmethod
    def _concat_same_type(cls, to_concat):
        """
        Required if Pandas tries to concatenate multiple arrays of the same dtype.
        """
        combined = []
        for arr in to_concat:
            combined.extend(arr._data)
        return cls(combined)

    # Required abstract properties / methods for Pandas extension arrays:
    @property
    def dtype(self):
        return self._dtype

    def __getitem__(self, item):
        """
        Indexing into the array. Could be a single integer or a slice.
        """
        if isinstance(item, slice):
            # Return a new ExtensionArray for slices
            return PythonObjectPandasArray(self._data[item])
        elif isinstance(item, int):
            # Return the actual single scalar item
            return self._data[item]
        else:
            # Optional: If you want to handle list-like or other advanced indexing
            # For example:
            return PythonObjectPandasArray([self._data[i] for i in item])

    def __setitem__(self, key, value):
        """
        Set one or more values in the array.

        Parameters
        ----------
        key : int, slice, or array-like
            Index or indices to set
        value : object or array-like
            Value(s) to set at specified indices
        """
        if isinstance(key, slice):
            self._data[key] = value
        elif isinstance(key, int):
            self._data[key] = value
        else:
            # Handle array-like indices
            for i, v in zip(key, value):
                self._data[i] = v

    def __len__(self):
        return len(self._data)

    def isna(self):
        """
        Boolean array indicating missing values. We treat None as missing.
        """
        return mp_utils.parallel_apply(data_utils.is_none, self._data)

    def __iter__(self):
        return iter(self._data)

    def copy(self):
        """
        Return copy of array
        """
        return PythonObjectPandasArray(np.copy(self._data))
