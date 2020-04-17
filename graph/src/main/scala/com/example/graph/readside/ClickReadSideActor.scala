package com.example.graph.readside

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.alpakka.cassandra.CassandraBatchSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.driver.core._
import com.example.graph.GraphNodeEntity.GraphNodeUpdated
import com.example.graph.config.ReadSideConfig

import scala.concurrent.ExecutionContextExecutor

object ClickReadSideActor {
  val ClickUpdateEventName = "clickupdate"
  case class ClickReadSideActorOffset(offset: Offset)

  def behaviour(
    readSideConfig: ReadSideConfig
  )(implicit session: Session): Behavior[ClickReadSideActorOffset] = Behaviors.setup[ClickReadSideActorOffset] { context =>
    implicit val system: ActorSystem[Nothing] = context.system
    implicit val ec: ExecutionContextExecutor = system.executionContext

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val nodeInsertStatement = session.prepare(s"INSERT INTO graph.nodes(type, id, tags, properties) VALUES (?, ?, ?, ?)")
    val nodeInsertBinder =
      (e: EventEnvelope, statement: PreparedStatement) => {
        import scala.collection.JavaConverters.mapAsJavaMap
        e.event match {
          case elemToInsert: GraphNodeUpdated =>
            println(elemToInsert)
            statement.bind(
              elemToInsert.nodeType,
              elemToInsert.id,
              if(elemToInsert.tags.isEmpty) null else mapAsJavaMap(elemToInsert.tags),
              if(elemToInsert.properties.isEmpty) null else mapAsJavaMap(elemToInsert.properties)
            )
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
            statement.bind(ClickUpdateEventName, uuid)
          case _ =>
            throw new RuntimeException("wrong offset type")
        }
      }

    val saveOffsetFlow = CassandraFlow.createWithPassThrough(
      readSideConfig.producerParallelism,
      offsetInsertStatement,
      offsetInsertBinder
    )

    val stmt = new SimpleStatement(s"SELECT * FROM graph.read_side_offsets WHERE tag = '$ClickUpdateEventName'").setFetchSize(1)

    val offsetQuery = CassandraSource(stmt)
      .map(row => Offset.timeBasedUUID(row.getUUID("offset")))
      .runWith(Sink.seq).map(_.headOption)

    offsetQuery.foreach {
      case Some(o) =>
        println(o)
        context.self ! ClickReadSideActorOffset(o)
      case None =>
        context.self ! ClickReadSideActorOffset(NoOffset)
    }


    val waiting: Behavior[ClickReadSideActorOffset] =
      Behaviors.receiveMessage {
        case ClickReadSideActorOffset(offset) =>
          println("x" * 100)
          val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(ClickUpdateEventName, offset)
          createdStream
            .via(saveNodeFlow)
            .via(saveOffsetFlow)
            .runWith(Sink.foreach(println))

          Behaviors.empty
      }

    waiting
  }
}
