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
        return store.getColumns(new int[]{0});
    }
}
