package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements ColumnarOperator {
    private ColumnStore store;

    // TODO: Add required structures

    public Scan(ColumnStore store) {
        // TODO: Implement
        this.store = store;
    }

    @Override
    public DBColumn[] execute() {
        final int length = store.getSchema().length;
        final int[] ints = new int[length];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = i;
        }

        return store.getColumns(ints);
    }
}

