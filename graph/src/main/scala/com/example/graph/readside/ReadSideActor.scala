package com.example.graph.readside

import akka.{Done, NotUsed}
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.alpakka.cassandra.CassandraBatchSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.driver.core._
import com.example.graph.GraphNodeEntity.GraphNodeUpdated
import com.example.graph.config.ReadSideConfig

import scala.collection.immutable
import scala.concurrent.{ExecutionContextExecutor, Future}

object ReadSideActor {
  val NodeUpdateEventName = "nodeupdate"
  case class ReadSideActorOffset(offset: Offset)

  def ReadSideActorBehaviour(
    readSideConfig: ReadSideConfig
  )(implicit session: Session): Behavior[ReadSideActorOffset] = Behaviors.setup[ReadSideActorOffset] { context =>
    implicit val system: ActorSystem[Nothing] = context.system
    implicit val ec: ExecutionContextExecutor = system.executionContext

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val nodeInsertStatement = session.prepare(s"INSERT INTO graph.nodes(type, id, properties) VALUES (?, ?, ?)")
    val nodeInsertBinder =
      (e: EventEnvelope, statement: PreparedStatement) => {
        e.event match {
          case elemToInsert: GraphNodeUpdated =>
            statement.bind(elemToInsert.nodeType, elemToInsert.id, if(elemToInsert.columns.isEmpty) null else elemToInsert.columns)
          case _ =>
            throw new RuntimeException("wrong message")
        }
      }
    val settings: CassandraBatchSettings = CassandraBatchSettings()

    val saveNodeFlow: Flow[EventEnvelope, EventEnvelope, NotUsed] = CassandraFlow.createWithPassThrough(
      readSideConfig.producerParallelism,
      nodeInsertStatement,
      nodeInsertBinder
    )

    val offsetInsertStatement = session.prepare(s"INSERT INTO graph.read_side_offsets(tag, offset) VALUES (?, ?)")
    val offsetInsertBinder =
      (e: EventEnvelope, statement: PreparedStatement) => {
        e.offset match {
          case TimeBasedUUID(uuid) =>
            statement.bind(NodeUpdateEventName, uuid)
          case _ =>
            throw new RuntimeException("wrong offset type")
        }
      }

    val saveOffsetFlow = CassandraFlow.createWithPassThrough(
      readSideConfig.producerParallelism,
      offsetInsertStatement,
      offsetInsertBinder
    )

    val stmt = new SimpleStatement(s"SELECT * FROM graph.read_side_offsets WHERE tag = '$NodeUpdateEventName'").setFetchSize(1)

    val offsetQuery = CassandraSource(stmt)
      .map(row => Offset.timeBasedUUID(row.getUUID("offset")))
      .runWith(Sink.seq).map(_.headOption)

    offsetQuery.foreach {
      case Some(o) =>
        context.self ! ReadSideActorOffset(o)
      case None =>
        context.self ! ReadSideActorOffset(NoOffset)
    }


    val waiting: Behavior[ReadSideActorOffset] =
      Behaviors.receiveMessage {
        case ReadSideActorOffset(offset) =>
          val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(NodeUpdateEventName, offset)
          createdStream
            .via(saveNodeFlow)
            .via(saveOffsetFlow)
            .runWith(Sink.foreach(println))

          Behaviors.empty
      }

    waiting
  }
}
