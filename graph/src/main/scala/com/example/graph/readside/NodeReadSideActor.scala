package com.example.graph.readside

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.alpakka.cassandra.CassandraBatchSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.driver.core._
import com.example.graph.GraphNodeEntity._
import com.example.graph.config.ReadSideConfig
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

object NodeReadSideActor {
  val NodeUpdateEventName = "nodeupdate"

  case class NodeInfo(company: String, nodeId: String, nodeType: String, tags: Map[String, Int], properties: Map[String, String])

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[ReadSideActorOffset], name = "ReadSideActorOffset"),
      new JsonSubTypes.Type(value = classOf[RetrieveAllNodes], name = "RetrieveAllNodes"),
      new JsonSubTypes.Type(value = classOf[NodeInformationUpdate], name = "NodeInformationUpdate"),
      new JsonSubTypes.Type(value = classOf[RelatedNodeQuery], name = "RelatedNodeQuery")))
  sealed trait NodeReadSideCommand

  case class ReadSideActorOffset(offset: Offset) extends NodeReadSideCommand
  case class RetrieveAllNodes(list: Seq[NodeInfo]) extends NodeReadSideCommand
  case class NodeInformationUpdate(node: NodeInfo) extends NodeReadSideCommand
  case class RelatedNodeQuery(tags: Tags, replyTo: ActorRef[RelatedNodeQueryResponse]) extends NodeReadSideCommand

  case class RelatedNodeQueryResponse(nodes: Map[String, Int])

  def ReadSideActorBehaviour(
    readSideConfig: ReadSideConfig,
    offsetManagement: OffsetManagement
  )(implicit session: Session): Behavior[NodeReadSideCommand] = Behaviors.withStash(100) { buffer =>
    Behaviors.setup[NodeReadSideCommand] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      val nodeInsertStatement = session.prepare(s"INSERT INTO graph.nodes(company, id, type, tags, properties) VALUES (?, ?, ?, ?, ?)")
      val nodeInsertBinder =
        (e: EventEnvelope, statement: PreparedStatement) => {
          import scala.collection.JavaConverters.mapAsJavaMap
          e.event match {
            case elemToInsert: GraphNodeUpdated =>
              statement.bind(
                elemToInsert.companyId,
                elemToInsert.id,
                elemToInsert.nodeType,
                if (elemToInsert.tags.isEmpty) null else mapAsJavaMap(elemToInsert.tags),
                if (elemToInsert.properties.isEmpty) null else mapAsJavaMap(elemToInsert.properties)
              )
            case elemToRemove: GraphNodeDisabled =>
              statement.bind(
                elemToRemove.companyId,
                elemToRemove.id,
                "disabled",
                null,
                null
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

      offsetManagement.offsetQuery(NodeUpdateEventName).foreach {
        case Some(o) =>
          context.self ! ReadSideActorOffset(o)
        case None =>
          context.self ! ReadSideActorOffset(NoOffset)
      }

      val nodesStmt = new SimpleStatement(s"SELECT * FROM graph.nodes")

      val nodesQuery = CassandraSource(nodesStmt)
        .map { row =>
          NodeInfo(
            row.getString("company"),
            row.getString("id"),
            row.getString("type"),
            row.getMap("tags", classOf[String], classOf[java.lang.Integer])
              .asScala.toMap.mapValues(_.toInt),
            row.getMap("properties", classOf[String], classOf[String]).asScala.toMap
          )
        }
        .runWith(Sink.seq)

      nodesQuery.foreach {
        context.self ! RetrieveAllNodes(_)
      }

      def collectNewNode(list: Seq[NodeInfo]): Behavior[NodeReadSideCommand] =
        Behaviors.receiveMessagePartial{
          case NodeInformationUpdate(node) =>
            collectNewNode(node +: list)

          case RelatedNodeQuery(tags, replyTo) =>
            val visitorTotalWeight = tags.values.sum + 1

            val recommended: Map[String, Int] = list.foldLeft(Seq.empty[(String, Int)]){
              case (acc, curNode) =>
                val weightTotal = curNode.tags.foldLeft(0){
                  case (weightAcc, (label, weight)) =>
                    weightAcc + (tags.getOrElse(label, 0) * (weight.toDouble / visitorTotalWeight)).toInt
                }
                (curNode.nodeId, weightTotal) +: acc
            }.take(20).toMap

            replyTo ! RelatedNodeQueryResponse(recommended)
            Behaviors.same
        }

      val waitingForInitialLoad: Behavior[NodeReadSideCommand] =
        Behaviors.receiveMessagePartial{
          case RetrieveAllNodes(list) =>
            context.log.debug(list.toString)
            buffer.unstashAll(collectNewNode(list))
          case x =>
            buffer.stash(x)
            Behaviors.same
        }

      val waiting: Behavior[NodeReadSideCommand] =
        Behaviors.receiveMessage {
          case ReadSideActorOffset(offset) =>
//            println(offset)
            val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(NodeUpdateEventName, offset)
            createdStream
              .map {
                case ee@EventEnvelope(_, _, _, value: GraphNodeUpdated) =>
//                  println(value)
                  context.self ! NodeInformationUpdate(NodeInfo(value.companyId, value.id, value.nodeType, value.tags, value.properties))
                  ee
                case ee =>
                  println(ee)
                  ee
              }
              .via(saveNodeFlow)
              .via(offsetManagement.saveOffsetFlow(readSideConfig.producerParallelism, NodeUpdateEventName))
              .runWith(Sink.ignore) //TODO: move on to the next message

            buffer.unstashAll(waitingForInitialLoad)

          case x =>
            buffer.stash(x)
            Behaviors.same
        }

      waiting
    }
  }
}
