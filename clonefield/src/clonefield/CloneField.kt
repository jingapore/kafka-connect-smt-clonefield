package clonefield

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig

open class CloneField<R: ConnectRecord<R>>(private val which: Which): Transformation<R> {
    enum class Which { KEY, VALUE }
    private var tx: ((R) -> R)? = null
    class Key<R : ConnectRecord<R>> : CloneField<R>(which = clonefield.CloneField.Which.KEY)
    class Value<R : ConnectRecord<R>> : CloneField<R>(which = clonefield.CloneField.Which.VALUE)
    override fun apply(p0: R?): R? {
        TODO("Not yet implemented")
    }

    override fun config(): ConfigDef? {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun configure(configs: Map<String?, *>?) {
        TODO("Not yet implemented")
    }
}