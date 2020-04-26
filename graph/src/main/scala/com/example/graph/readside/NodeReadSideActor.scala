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

object NodeReadSideActor {
  val NodeUpdateEventName = "nodeupdate"
  case class ReadSideActorOffset(offset: Offset)

  def ReadSideActorBehaviour(
    readSideConfig: ReadSideConfig,
    offsetManagement: OffsetManagement
  )(implicit session: Session): Behavior[ReadSideActorOffset] = Behaviors.setup[ReadSideActorOffset] { context =>
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

    val stmt = new SimpleStatement(s"SELECT * FROM graph.read_side_offsets WHERE tag = '$NodeUpdateEventName'").setFetchSize(1)

    val offsetQuery = CassandraSource(stmt)
      .map(row => Offset.timeBasedUUID(row.getUUID("offset")))
      .runWith(Sink.seq).map(_.headOption)

    offsetQuery.foreach {
      case Some(o) =>
        println(o)
        context.self ! ReadSideActorOffset(o)
      case None =>
        context.self ! ReadSideActorOffset(NoOffset)
    }


    val waiting: Behavior[ReadSideActorOffset] =
      Behaviors.receiveMessage {
        case ReadSideActorOffset(offset) =>
          println("x" * 100)
          val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(NodeUpdateEventName, offset)
          createdStream
            .via(saveNodeFlow)
            .via(offsetManagement.saveOffsetFlow(readSideConfig.producerParallelism, NodeUpdateEventName))
            .runWith(Sink.foreach(println)) //TODO: move on to the next message

          Behaviors.empty
      }

    waiting
  }
}
