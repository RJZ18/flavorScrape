from avro import schema, datafile, io
import pprint

OUTFILE_NAME = r'C:\Data\product.avro'
INPUT_SCHEMA_NAME = r"C:\Data\product.avsc"

# Open a file
fo = open(INPUT_SCHEMA_NAME, "r+")
SCHEMA_STR = fo.read();
print("Read String is : ", SCHEMA_STR)
# Close opend file
fo.close()

SCHEMA = schema.Parse(SCHEMA_STR)


# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

# Create a 'data file' (avro file) writer
df_writer = datafile.DataFileWriter(
    open(OUTFILE_NAME, 'wb'),
    rec_writer,
    writer_schema=SCHEMA
)

df_writer.append({"product_id": 1000, "product_name": "Hugo Boss XY"})


df_writer.close()