package clonefield

import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.errors.DataException

internal fun <R : ConnectRecord<R>> keyCloneField(cfg: Config): (R) -> R = recordTransfomer(cfg, Lens.Key)
internal fun <R : ConnectRecord<R>> valueCloneField(cfg: Config): (R) -> R = recordTransfomer(cfg, Lens.Value)

private sealed interface Lens {
    fun <R : ConnectRecord<R>> getSchema(r: R): Schema?
    fun <R : ConnectRecord<R>> getValue(r: R): Any?
    fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R

    data object Key : Lens {
        override fun <R : ConnectRecord<R>> getValue(r: R): Any? = r.key()
        override fun <R : ConnectRecord<R>> getSchema(r: R): Schema? = r.keySchema()
        override fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R {
            return r.newRecord(r.topic(), r.kafkaPartition(), schema, value, r.valueSchema(), r.value(), r.timestamp())
        }
    }

    data object Value : Lens {
        override fun <R : ConnectRecord<R>> getValue(r: R): Any? = r.value()
        override fun <R : ConnectRecord<R>> getSchema(r: R): Schema? = r.valueSchema()
        override fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R {
            return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, value, r.timestamp())
        }
    }
}

private fun <R : ConnectRecord<R>> recordTransformer(cfg: Config, lens: Lens): (R) -> R {
    val schemaCache = SynchronizedCache<Schema, Schema>(LRUCache<Schema, Schema>(CACHE_SIZE))
    return { record ->
        val schema = lens.getSchema(record) ?: throw DataException("Cannot apply without schema")
        val value = lens.getValue(record)
        if (value == null) {
            record
        } else {
            val (newSchema, newValue) =
            val updatedSchema =
                schemaCache.get(schema) ?: buildUpdatedSchema(schema, cfg).also { schemaCache.put(schema, it) }
        }
    }

}

private fun <K, V> Cache<K, V>.getOrPut(key: K, defaultVal: () -> V): V = get(key) ?: defaultVal().also { put(key, it) }

private fun buildUpdatedSchema(original: Schema, cfg: Config): Schema {
    if (original.type() != Schema.Type.STRUCT) {
        throw DataException(
            "you are cloning fields on a struct, but the schema you had passed has type: ${original.type()}"
        )
    }

    val builder = SchemaBuilder.struct()
        .name(original.name())
        .version(original.version())
        .doc(original.doc())

    original.fields().forEach { builder.field(it.name(), it.schema()) }
    builder.field(cfg.to, original.field(cfg.from).schema())

    return builder.build()
}
