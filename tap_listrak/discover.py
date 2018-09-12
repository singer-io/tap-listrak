from singer.catalog import Catalog, CatalogEntry, Schema

from tap_listrak.schema import get_schemas, PKS

def discover():
    schemas = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        pk = PKS[stream_name]

        metadata = []
        for field_name in schema_dict['properties'].keys():
            if field_name in pk:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', field_name]
            })

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=metadata
        ))

    return catalog