package test;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

public class GetTable {

    public static Table getTable(String db,String table) {
        Identifier identifier = Identifier.create(db, table);
        try {
            Catalog catalog = CreateCatalog.createFilesystemCatalog();
            return catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }
}