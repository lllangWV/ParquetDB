from tempfile import mkdtemp

from parquetdb import ParquetDatasetDB
import pyarrow.compute as pc

initial = {
		"global": [
			{
				"key": b"\xb4_lise_schema_version",
				"value": b"\x00",
			},
			{"key": b"\xabmain_branch", "value": b"\xa5trunk"},
			{"key": b"\xa6branch", "value": b"\xa5trunk"},
			{"key": b"\xa4turn", "value": b"\x00"},
			{"key": b"\xa4tick", "value": b"\x00"},
			{"key": b"\xa8language", "value": b"\xa3eng"},
		]
	}

dir = mkdtemp()
dir = "C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB_Dev"
db = ParquetDatasetDB(dataset_name="global", dir=dir, n_cores=1)

def set_global(key: bytes, value: bytes):
	try:
		id_ = db.read(filters=[pc.field("key") == key], columns=["id"])["id"][0].as_py()
		db.update(
			{"id": id_, "key": key, "value": value}, "global"
		)
	except:
		db.create({"key": key, "value": value})


set_global(b"\xa6branch", b"\xa5tronc")
set_global(b"\xa4turn", b"\x00")

set_global(b"\xa8language", b"\xa3eng")


set_global(b"\xa4turn",b"\xa5tronc")

# db.create(initial, "global")

df = db.read().to_pandas()
print(df)