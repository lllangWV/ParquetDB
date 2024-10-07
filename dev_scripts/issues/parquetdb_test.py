from tempfile import mkdtemp
import logging

from parquetdb import ParquetDB
import pyarrow.compute as pc

logger=logging.getLogger('parquetdb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


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
db = ParquetDB(dir)

def set_global(key: bytes, value: bytes):
	try:
		id_ = db.read(dataset_name="global", filters=[pc.field("key") == key], columns=["id"])["id"][0].as_py()
		db.update(
			{"id": id_, "key": key, "value": value}, "global"
		)
	except:
		db.create({"key": key, "value": value}, "global")


set_global(b"\xa6branch", b"\xa5tronc")
set_global(b"\xa4turn", b"\x00")

df = db.read(dataset_name="global").to_pandas()
print(df)
set_global(b"\xa6branch", b"\x00")
print('-'*200)

df = db.read(dataset_name="global").to_pandas()
print(df)