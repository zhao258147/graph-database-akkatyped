package com.example.graph.readside

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.query.{EventEnvelope, Offset, TimeBasedUUID}
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSource}
import akka.stream.scaladsl.{Flow, Sink}
import com.datastax.driver.core.{PreparedStatement, Session, SimpleStatement}

import scala.concurrent.ExecutionContext

class OffsetManagement(implicit session: Session, ec: ExecutionContext, system: ActorSystem[Nothing]) {
  private lazy val offsetInsertStatement = session.prepare(s"INSERT INTO graph.read_side_offsets(tag, offset) VALUES (?, ?)")
  private def offsetInsertBinder(tag: String) =
    (e: EventEnvelope, statement: PreparedStatement) => {
      e.offset match {
        case TimeBasedUUID(uuid) =>
          statement.bind(tag, uuid)
        case _ =>
          throw new RuntimeException("wrong offset type")
      }
    }

  def saveOffsetFlow(parallelism: Int, tag: String): Flow[EventEnvelope, EventEnvelope, NotUsed] = CassandraFlow.createWithPassThrough(
    parallelism,
    offsetInsertStatement,
    offsetInsertBinder(tag)
  )

  private def offsetStmt(tag: String) = new SimpleStatement(s"SELECT * FROM graph.read_side_offsets WHERE tag = '$tag'").setFetchSize(1)
  def offsetQuery(tag: String) = CassandraSource(offsetStmt(tag))
    .map(row => Offset.timeBasedUUID(row.getUUID("offset")))
    .runWith(Sink.seq).map(_.headOption)
}
