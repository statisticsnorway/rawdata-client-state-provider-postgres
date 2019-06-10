import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.provider.state.postgres.PostgresStatePersistenceInitializer;

module no.ssb.rawdata.state.provider.postgres {
    requires java.base;
    requires java.logging;
    requires org.slf4j;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires java.sql;
    requires com.zaxxer.hikari;
    requires postgresql;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;

    provides StatePersistenceInitializer with PostgresStatePersistenceInitializer;

    opens postgres;

    exports no.ssb.rawdata.provider.state.postgres;

}
