package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;
import java.util.List;

public class Join implements ColumnarOperator {
    private final ColumnarOperator leftChild;
    private final ColumnarOperator rightChild;
    private final int leftFieldNo;
    private final int rightFieldNo;

    // TODO: Add required structures

    public Join(ColumnarOperator leftChild,
                ColumnarOperator rightChild,
                int leftFieldNo,
                int rightFieldNo) {
        // TODO: Implement
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftFieldNo = leftFieldNo;
        this.rightFieldNo = rightFieldNo;
    }

    public DBColumn[] execute() {
        final DBColumn[] left = leftChild.execute();
        final DBColumn[] right = rightChild.execute();

        // TODO: Implement
        return null;
    }
}
