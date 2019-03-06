package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

import java.util.List;

public class DBColumn {

    // TODO: Implement
    private int column;
    private Object[] values;

    public DBColumn(int column, DataType dataType, List<Object> objects) {
        this.column = column;
        values = objects.toArray(new Object[]{});
    }

    public Integer[] getAsInteger() {
        final Integer[] integers = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object object = values[i];
            integers[i] = Integer.valueOf(object.toString());
        }
        return integers;
    }
}
