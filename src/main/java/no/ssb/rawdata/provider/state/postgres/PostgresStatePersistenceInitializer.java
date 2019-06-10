package no.ssb.rawdata.provider.state.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.ssb.config.DynamicConfiguration;
import no.ssb.rawdata.api.state.StatePersistence;
import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.api.util.FileAndClasspathReaderUtils;
import no.ssb.service.provider.api.ProviderName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

@ProviderName("postgres")
public class PostgresStatePersistenceInitializer implements StatePersistenceInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresStatePersistenceInitializer.class);

    @Override
    public String providerId() {
        return "postgres";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "postgres.driver.host",
                "postgres.driver.port",
                "postgres.driver.user",
                "postgres.driver.password",
                "postgres.driver.database"
        );
    }

    @Override
    public StatePersistence initialize(DynamicConfiguration configuration) {
        HikariDataSource dataSource = openDataSource(configuration);
        return new PostgresStatePersistence(new PostgresTransactionFactory(dataSource));
    }

    public static HikariDataSource openDataSource(DynamicConfiguration configuration) {
        Map<String, String> configMap = configuration.asMap();
        String postgresDbDriverHost = configMap.get("postgres.driver.host");
        String postgresDbDriverPort = configMap.get("postgres.driver.port");
        HikariDataSource dataSource = PostgresStatePersistenceInitializer.openDataSource(
                postgresDbDriverHost,
                postgresDbDriverPort,
                configMap.get("postgres.driver.user"),
                configMap.get("postgres.driver.password"),
                configMap.get("postgres.driver.database"),
                configuration.evaluateToBoolean("postgres.dropOrCreateDb")
        );
        return dataSource;
    }

    // https://github.com/brettwooldridge/HikariCP
    static HikariDataSource openDataSource(String postgresDbDriverHost, String postgresDbDriverPort, String postgresDbDriverUser, String postgresDbDriverPassword, String postgresDbDriverDatabase, boolean dropOrCreateDb) {
        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", postgresDbDriverHost);
        props.setProperty("dataSource.portNumber", postgresDbDriverPort);
        props.setProperty("dataSource.user", postgresDbDriverUser);
        props.setProperty("dataSource.password", postgresDbDriverPassword);
        props.setProperty("dataSource.databaseName", postgresDbDriverDatabase);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        if (dropOrCreateDb) {
            dropOrCreateDatabase(datasource);
        }

        return datasource;
    }

    static void dropOrCreateDatabase(HikariDataSource datasource) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource("postgres/init-db.sql");
            System.out.printf("initSQL: %s%n", initSQL);
            Connection conn = datasource.getConnection();
            conn.beginRequest();

            try (Scanner s = new Scanner(initSQL)) {
                s.useDelimiter("(;(\r)?\n)|(--\n)");
                try (Statement st = conn.createStatement()) {
                    try {
                        while (s.hasNext()) {
                            String line = s.next();
                            if (line.startsWith("/*!") && line.endsWith("*/")) {
                                int i = line.indexOf(' ');
                                line = line.substring(i + 1, line.length() - " */".length());
                            }

                            if (line.trim().length() > 0) {
                                st.execute(line);
                            }
                        }
                        conn.commit();
                    } finally {
                        st.close();
                    }
                }
            }
            conn.endRequest();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
