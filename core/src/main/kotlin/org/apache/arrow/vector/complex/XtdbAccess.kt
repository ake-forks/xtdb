package org.apache.arrow.vector.complex

import org.apache.arrow.vector.FieldVector

internal fun StructVector.replaceChild(child: FieldVector) = apply {
    putChild(child.name, child)
}