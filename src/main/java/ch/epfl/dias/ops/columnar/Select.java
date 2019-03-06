package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Select implements ColumnarOperator {
    private final ColumnarOperator child;
    private final BinaryOp op;
    private final int fieldNo;
    private final int value;

    // TODO: Add required structures

    public Select(ColumnarOperator child,
                  BinaryOp op,
                  int fieldNo,
                  int value) {
        // TODO: Implement
        this.child = child;
        this.op = op;
        this.fieldNo = fieldNo;
        this.value = value;
    }

    @Override
    public DBColumn[] execute() {
        final List<DBColumn> collect = Arrays.stream(child.execute())
            .filter(o -> {
                final Integer[] asInteger = o.getAsInteger();
                return match(asInteger[0], value);
            })
            .collect(Collectors.toList());
        return collect.toArray(new DBColumn[]{});
    }

    private boolean match(Integer valueFromDB, Integer valueFromUser) {
        switch (op) {
            case LT:
                return valueFromDB <= valueFromDB;
            case LE:
                return valueFromDB < valueFromDB;
            case EQ:
                return valueFromDB == valueFromDB;
            case NE:
                return valueFromDB != valueFromDB;
            case GT:
                return valueFromDB > valueFromDB;
            case GE:
                return valueFromDB >= valueFromDB;
        }
        throw new RuntimeException("WRong");
    }
}
