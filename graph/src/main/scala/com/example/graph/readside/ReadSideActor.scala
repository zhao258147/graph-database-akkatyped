package com.example.graph.readside

import akka.{Done, NotUsed}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.internal.protobuf.ShardingMessages
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.alpakka.cassandra.CassandraBatchSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.driver.core._
import com.example.graph.GraphNodeEntity.{EdgeDirection, EdgeProperties, EdgeType, GraphNodeCommand, GraphNodeCommandReply, GraphNodeUpdated, MoveEvent, NodeId, RemoveEdgeCommand, To, UpdateEdgeCommand}
import com.example.graph.config.ReadSideConfig

import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContextExecutor, Future}

object ReadSideActor {
  val NodeUpdateEventName = "nodeupdate"
  val MoveEventName = "moveevent"

  sealed trait RSACommand
  case class ReadSideActorOffset(offset: Offset) extends RSACommand
  case class MoveOffset(offset: Offset) extends RSACommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends RSACommand

  def ReadSideActorBehaviour(
    readSideConfig: ReadSideConfig,
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand]]
  )(implicit session: Session): Behavior[RSACommand] = Behaviors.setup[RSACommand] { context =>
    implicit val system: ActorSystem[Nothing] = context.system
    implicit val ec: ExecutionContextExecutor = system.executionContext

    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      context.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val nodeInsertStatement = session.prepare(s"INSERT INTO graph.nodes(type, id, properties) VALUES (?, ?, ?)")
    val nodeInsertBinder =
      (e: EventEnvelope, statement: PreparedStatement) => {
        e.event match {
          case elemToInsert: GraphNodeUpdated =>
            statement.bind(
              elemToInsert.nodeType,
              elemToInsert.id,
              if(elemToInsert.properties.isEmpty) null else elemToInsert.properties
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

    val moveoffsetInsertStatement = session.prepare(s"INSERT INTO graph.read_side_offsets(tag, offset) VALUES (?, ?)")
    val moveoffsetInsertBinder =
      (e: EventEnvelope, statement: PreparedStatement) => {
        e.offset match {
          case TimeBasedUUID(uuid) =>
            statement.bind(MoveEventName, uuid)
          case _ =>
            throw new RuntimeException("wrong offset type")
        }
      }

    val movesaveOffsetFlow = CassandraFlow.createWithPassThrough(
      readSideConfig.producerParallelism,
      moveoffsetInsertStatement,
      moveoffsetInsertBinder
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

    val movestmt = new SimpleStatement(s"SELECT * FROM graph.read_side_offsets WHERE tag = '$MoveEventName'").setFetchSize(1)

    val moveoffsetQuery = CassandraSource(movestmt)
      .map(row => Offset.timeBasedUUID(row.getUUID("offset")))
      .runWith(Sink.seq).map(_.headOption)

    moveoffsetQuery.foreach {
      case Some(o) =>
        context.self ! MoveOffset(o)
      case None =>
        context.self ! MoveOffset(NoOffset)
    }

//    val qNodes: mutable.Map[String, Int] = mutable.Map.empty
    var earthX: Int = 0
    var earthY: Int = 0
    def findQ(x: Int, y: Int) = {
      if(x > 0 && y > 0) 1
      else if (x < 0 && y > 0) 2
      else if (x < 0 && y < 0) 3
      else 4
    }

    val waiting: Behavior[RSACommand] =
      Behaviors.receiveMessagePartial {
        case ReadSideActorOffset(offset) =>
          val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(NodeUpdateEventName, offset)
          createdStream
            .via(saveNodeFlow)
            .via(saveOffsetFlow)
            .runWith(Sink.foreach(println))

          Behaviors.same

        case MoveOffset(offset) =>
          println("move offset " * 50)
          val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(MoveEventName, offset)
          createdStream
            .map{ evt: EventEnvelope =>
              println(evt)
              evt.event match {
                case mv: MoveEvent =>
                  if(mv.id == "Earth") {
                    earthX = mv.x
                    earthY = mv.y
                  } else if (mv.id == "Mars") {
                    val dis = math.pow(mv.x - earthX, 2) + math.pow(mv.y - earthY, 2)
                    println("distance " * 10)
                    println(dis)
                    if (dis < 12000) {
                      graphCordinator ! ShardingEnvelope(
                        mv.id,
                        UpdateEdgeCommand(mv.id, "shortest", To("Earth"), Map.empty, nodeEntityResponseMapper)
                      )
                    } else {
                      println("remove edge")
                      graphCordinator ! ShardingEnvelope(
                        mv.id,
                        RemoveEdgeCommand(mv.id, "Earth", "shortest", nodeEntityResponseMapper)
                      )
                    }
                  }
              }
              evt
            }
            .via(movesaveOffsetFlow)
            .runWith(Sink.foreach(println))

          Behaviors.same
      }

    waiting
  }
}
