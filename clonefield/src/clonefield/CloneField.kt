package clonefield

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig

open class CloneField<R : ConnectRecord<R>>(private val which: Which) : Transformation<R> {
    enum class Which { KEY, VALUE }

    private var tx: ((R) -> R)? = null

    class Key<R : ConnectRecord<R>> : CloneField<R>(which = Which.KEY)
    class Value<R : ConnectRecord<R>> : CloneField<R>(which = Which.VALUE)

    override fun apply(record: R): R = tx?.invoke(record) ?: throw IllegalStateException("CloneField not configured")


    override fun config(): ConfigDef? = CONFIG_DEF

    override fun close() {}

    override fun configure(configs: Map<String?, *>?) {
        val simpleConfig = SimpleConfig(CONFIG_DEF, configs)
        val cfg = Config(
            from = simpleConfig.getString(FROM_FIELDNAME),
            to = simpleConfig.getString(TO_FIELDNAME)
        )
        tx = when (which) {
            Which.KEY -> keyCloneField(cfg)
            Which.VALUE -> valueCloneField(cfg)
        }
    }
}