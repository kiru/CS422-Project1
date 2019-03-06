package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;

public class ProjectAggregate implements ColumnarOperator {
	private final ColumnarOperator child;
	private final Aggregate agg;
	private final DataType dt;
	private final int fieldNo;

	// TODO: Add required structures
	
	public ProjectAggregate(ColumnarOperator child,
							Aggregate agg,
							DataType dt,
							int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		final DBColumn[] execute = child.execute();

		switch (agg){
			case COUNT:
				return new DBColumn[]{
					new DBColumn(0, DataType.INT, Arrays.asList(execute.length))
				};
			case SUM:
                throw new RuntimeException("ja");
			case MIN:
				throw new RuntimeException("ja");
			case MAX:
				throw new RuntimeException("ja");
			case AVG:
				throw new RuntimeException("ja");
		}

		// TODO: Implement
		return null;
	}
}
