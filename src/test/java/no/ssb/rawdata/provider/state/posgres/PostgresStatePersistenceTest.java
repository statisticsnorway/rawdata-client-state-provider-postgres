package no.ssb.rawdata.provider.state.posgres;

import io.reactivex.Flowable;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.provider.state.postgres.PostgresStatePersistence;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class PostgresStatePersistenceTest {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresStatePersistenceTest.class);

    private DynamicConfiguration configuration;
    private PostgresStatePersistence stateProvider;

    static DynamicConfiguration configuration() {
        Path currentPath = Paths.get("").toAbsolutePath().resolve("target");
        return new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("application-test.properties")
                .values("storage.provider", "postgres")
                .values("postgres.driver.host", "localhost")
                .values("postgres.driver.port", "5432")
                .values("postgres.driver.user", "rdc")
                .values("postgres.driver.password", "rdc")
                .values("postgres.driver.database", "rawdata_client")
                .build();
    }

    static PostgresStatePersistence storageProvider(DynamicConfiguration configuration) {
        return ProviderConfigurator.configure(configuration, "postgres", StatePersistenceInitializer.class);
    }

    @BeforeClass
    public void setUp() {
        configuration = configuration();
        stateProvider = storageProvider(configuration);
        assertNotNull(stateProvider);
    }

    @Test(enabled = false)
    public void testName() throws InterruptedException {
        stateProvider.trackCompletedPositions("ns", List.of("a", "b")).blockingGet();
        stateProvider.trackCompletedPositions("ns", List.of("c", "d", "e", "f", "g", "h")).blockingGet();

        System.out.printf("first: %s%n", stateProvider.getFirstPosition("ns").blockingGet());
        System.out.printf("last: %s%n", stateProvider.getLastPosition("ns").blockingGet());

        System.out.printf("offset: %s%n", stateProvider.getOffsetPosition("ns", "b", 3).blockingGet());

        stateProvider.readPositions("ns", "b", "e").subscribe(onNext -> System.out.printf("%s%n", onNext.position), onError -> onError.printStackTrace());

        Thread.sleep(250);
    }

    @Test //(enabled = false)
    public void testTx() {
        assertTrue(stateProvider.trackCompletedPositions("ns", List.of("a", "b")).blockingGet());
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "b");
        assertEquals(stateProvider.getOffsetPosition("ns", "a", 1).blockingGet(), "b");

        assertTrue(stateProvider.trackCompletedPositions("ns", List.of("c", "d", "e")).blockingGet());
        assertEquals(stateProvider.getLastPosition("ns").blockingGet(), "e");
        assertEquals(stateProvider.getOffsetPosition("ns", "c", 3).blockingGet(), "e");

        Flowable<CompletedPosition> flowable = stateProvider.readPositions("ns", "b", "e");
        List<String> expected = new ArrayList<>(List.of("b", "c", "d", "e"));
        flowable.subscribe(onNext -> expected.remove(onNext.position), onError -> onError.printStackTrace());
        assertTrue(expected.isEmpty());
    }

}

