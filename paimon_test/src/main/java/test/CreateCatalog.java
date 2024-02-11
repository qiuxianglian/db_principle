package test;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

public class CreateCatalog {


    public static void main(String[] args) throws Exception {

        try(Catalog filesystemCatalog = createFilesystemCatalog()){
            filesystemCatalog.createDatabase("t1_db",true);


            Schema.Builder schemaBuilder = Schema.newBuilder();
            schemaBuilder.primaryKey("f0", "f1");
            schemaBuilder.partitionKeys("f1");
            schemaBuilder.column("f0", DataTypes.STRING());
            schemaBuilder.column("f1", DataTypes.INT());
            Schema schema = schemaBuilder.build();

            Identifier identifier = Identifier.create("t1_db", "t1");
            try {
                filesystemCatalog.createTable(identifier, schema, false);
            } catch (Catalog.TableAlreadyExistException | Catalog.DatabaseNotExistException e) {
                e.printStackTrace();
                // do something
            }


            System.out.println(filesystemCatalog);
        }
    }
    public static Catalog createFilesystemCatalog() {
        CatalogContext context = CatalogContext.create(new Path("db"));
        return CatalogFactory.createCatalog(context);
    }

}