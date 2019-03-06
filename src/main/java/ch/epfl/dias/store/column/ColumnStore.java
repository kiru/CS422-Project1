package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ColumnStore extends Store {
    private final DataType[] schema;
    private final String filename;
    private final String delimiter;
    private final boolean lateMaterialization;

    // TODO: Add required structures

    private List<DBColumn> data = new ArrayList<>();


    public ColumnStore(DataType[] schema,
                       String filename,
                       String delimiter) {
        this(schema, filename, delimiter, false);
    }

    public ColumnStore(DataType[] schema,
                       String filename,
                       String delimiter,
                       boolean lateMaterialization) {
        // TODO: Implement
        this.schema = schema;
        this.filename = filename;
        this.delimiter = delimiter;
        this.lateMaterialization = lateMaterialization;
    }

    @Override
    public void load() throws IOException {

        List<List<Object>> valuesPerColumn = new ArrayList<>();
        for (DataType dataType : schema) {
            valuesPerColumn.add(new ArrayList<>());
        }

        Files.lines(new File(filename).toPath(), Charset.forName("UTF-8"))
            .forEach(eachLine -> {
                final String[] split = eachLine.split(delimiter);
                for (int i = 0; i < split.length; i++) {
                    String eachRow = split[i];
                    valuesPerColumn.get(i).add(eachRow);
                }
            });

        for (int i = 0; i < valuesPerColumn.size(); i++) {
            List<Object> objects = valuesPerColumn.get(i);
            final DataType dataType = schema[i];
            final DBColumn dbColumn = new DBColumn(i, dataType, objects);
            data.add(dbColumn);
        }
    }

    @Override
    public DBColumn[] getColumns(int[] columnsToGet) {
        final DBColumn[] dbColumns = new DBColumn[columnsToGet.length];

        for (int i1 = 0; i1 < columnsToGet.length; i1++) {
            int colIndex = columnsToGet[i1];
            dbColumns[i1] = data.get(colIndex);
        }

        return dbColumns;
    }
}
