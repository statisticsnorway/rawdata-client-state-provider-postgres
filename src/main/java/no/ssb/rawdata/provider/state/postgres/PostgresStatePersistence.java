package no.ssb.rawdata.provider.state.postgres;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.rawdata.api.persistence.CompletedPositionPublisher;
import no.ssb.rawdata.api.persistence.PersistenceException;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.StatePersistence;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class PostgresStatePersistence implements StatePersistence {

    private final PostgresTransactionFactory transactionFactory;

    public PostgresStatePersistence(PostgresTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    PostgresTransactionFactory getTransactionFactory() {
        return transactionFactory;
    }

    @Override
    public void close() throws IOException {
        transactionFactory.close();
    }

    @Override
    public Single<Boolean> trackCompletedPositions(String namespace, List<String> completedPositions) {
        return Single.fromCallable(() -> {
            try (PostgresTransaction tx = transactionFactory.createTransaction(false)) {
                try {
                    PreparedStatement ps = tx.connection.prepareStatement("INSERT INTO completed_positions (namespace, opaque_id, ts) VALUES (?, ?, ?)");
                    int n = 1;
                    for (String completedPosition : completedPositions) {
                        ps.setString(1, namespace);
                        ps.setString(2, completedPosition);
                        ps.setTimestamp(3, Timestamp.from(ZonedDateTime.now().toInstant()));
                        ps.addBatch();
                        n++;
                    }
                    return ps.executeBatch().length == n - 1;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Maybe<String> getFirstPosition(String namespace) {
        return Maybe.fromCallable(() -> {
            try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
                try {
                    PreparedStatement ps = tx.connection.prepareStatement("SELECT opaque_id FROM completed_positions WHERE namespace=? ORDER BY id LIMIT 1");
                    ps.setString(1, namespace);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Maybe<String> getLastPosition(String namespace) {
        return Maybe.fromCallable(() -> {
            try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
                try {
                    PreparedStatement ps = tx.connection.prepareStatement("SELECT opaque_id FROM completed_positions WHERE namespace=? ORDER BY id DESC LIMIT 1");
                    ps.setString(1, namespace);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }

    @Override
    public Maybe<String> getOffsetPosition(String namespace, String fromPosition, int offset) {
        return Maybe.fromCallable(() -> {
            try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
                try {
                    PreparedStatement ps = tx.connection.prepareStatement("SELECT DISTINCT id, opaque_id FROM completed_positions WHERE namespace = ? AND id > (SELECT id FROM completed_positions WHERE namespace = ? AND opaque_id = ?) ORDER BY id LIMIT ?",
                            ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY
                    );
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setString(3, fromPosition);
                    ps.setInt(4, offset);
                    ResultSet rs = ps.executeQuery();
                    if (rs.last()) {
                        return rs.getString(2);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        });
    }


    @Override
    public Flowable<CompletedPosition> readPositions(String namespace, String fromPosition, String toPosition) {
        return Single.fromCallable(() -> {
            try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
                try {
                    PreparedStatement ps = tx.connection.prepareStatement("SELECT id, opaque_id FROM completed_positions WHERE namespace = ? AND id >= (SELECT id FROM completed_positions WHERE namespace = ? AND opaque_id = ?) AND id <= (SELECT id FROM completed_positions WHERE namespace = ? AND opaque_id = ?) ORDER BY id");
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setString(3, fromPosition);
                    ps.setString(4, namespace);
                    ps.setString(5, toPosition);
                    ResultSet resultSet = ps.executeQuery();
                    Deque<CompletedPosition> result = new LinkedList<>();
                    while (resultSet.next()) {
                        String position = resultSet.getString(2);
                        result.add(new CompletedPosition(namespace, position));
                    }
                    return result;
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }).flatMapPublisher(result -> Flowable.fromPublisher(new CompletedPositionPublisher(result))
        );
    }
}
