package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
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
        final DBColumn[] execute = child.execute();
        final DBColumn dbColumn = execute[fieldNo];

        final Integer[] asInteger = dbColumn.getAsInteger();

        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < asInteger.length; i++) {
            Integer integer = asInteger[i];
            if (match(integer, value)) {
                indexes.add(i);
            }
        }

        final DBColumn[] copy = Arrays.copyOf(execute, execute.length);
        for (int i = 0; i < copy.length; i++) {
            DBColumn column = copy[i];
            copy[i] = removeByArray(column, indexes);
        }

        return copy;
    }


    public DBColumn removeByArray(DBColumn input, List<Integer> indexes) {
        final Object[] original = input.getValues();
        final Object[] changed = new Object[indexes.size()];

        int i = 0;
        for (Integer index : indexes) {
            changed[i++] = original[index];
        }

        return new DBColumn(0, DataType.INT, Arrays.asList(changed));
    }


    private boolean match(Integer valueFromDB, Integer valueFromUser) {
        switch (op) {
            case LT:
                return valueFromDB <= valueFromUser;
            case LE:
                return valueFromDB < valueFromUser;
            case EQ:
                return valueFromDB.equals(valueFromUser);
            case NE:
                return valueFromDB != valueFromUser;
            case GT:
                return valueFromDB > valueFromUser;
            case GE:
                return valueFromDB >= valueFromUser;
        }
        throw new RuntimeException("WRong");
    }
}
