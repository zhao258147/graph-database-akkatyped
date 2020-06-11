package com.example.graph.readside

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.alpakka.cassandra.CassandraBatchSettings
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.datastax.driver.core._
import com.example.graph.GraphNodeEntity.GraphNodeClickUpdated
import com.example.graph.config.ReadSideConfig
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}

object ClickReadSideActor {
  val ClickUpdateEventName = "clickupdate"

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[ClickReadSideActorOffset], name = "ClickReadSideActorOffset"),
      new JsonSubTypes.Type(value = classOf[NodeClickInfo], name = "NodeClickInfo"),
      new JsonSubTypes.Type(value = classOf[OnStartNodeClickInfo], name = "OnStartNodeClickInfo"),
      new JsonSubTypes.Type(value = classOf[TrendingNodesCommand], name = "TrendingNodesCommand")))
  sealed trait ClickStatCommands

  case class ClickReadSideActorOffset(offset: Offset) extends ClickStatCommands

  case class NodeClickInfo(company: String, nodeId: String, tags: Set[String], ts: Long, clicks: Int) extends ClickStatCommands

  case class OnStartNodeClickInfo(list: Seq[NodeClickInfo]) extends ClickStatCommands
  case class TrendingNodesCommand(tags: Set[String], replyTo: ActorRef[TrendingNodesResponse]) extends ClickStatCommands

  case class TrendingNodesResponse(overallRanking: Map[String, Int], rankingByType: Map[String, Seq[(String, Int)]])

  def behaviour(
    readSideConfig: ReadSideConfig,
    offsetManagement: OffsetManagement
  )(implicit session: Session): Behavior[ClickStatCommands] = Behaviors.withStash(100) { buffer =>
    Behaviors.setup[ClickStatCommands] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

//      println("ClickReadSideActor started")
      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      val clickInsertStatement = session.prepare(s"INSERT INTO graph.clicks(company, id, ts, type, tags, clicks) VALUES (?, ?, ?, ?, ?, ?)")
      val clickInsertBinder =
        (e: EventEnvelope, statement: PreparedStatement) => {
          e.event match {
            case elemToInsert: GraphNodeClickUpdated =>
              context.log.debug(elemToInsert.toString)
              statement.bind(
                elemToInsert.companyId,
                elemToInsert.nodeId,
                new java.lang.Long(elemToInsert.ts),
                elemToInsert.nodeType,
                if(elemToInsert.tags.isEmpty) null else mapAsJavaMap(elemToInsert.tags),
                new Integer(elemToInsert.clicks)
              )
            case _ =>
              throw new RuntimeException("wrong message")
          }
        }
      val settings: CassandraBatchSettings = CassandraBatchSettings()

      val saveClickFlow: Flow[EventEnvelope, EventEnvelope, NotUsed] = CassandraFlow.createWithPassThrough(
        readSideConfig.producerParallelism,
        clickInsertStatement,
        clickInsertBinder
      )

      offsetManagement.offsetQuery(ClickUpdateEventName).foreach {
        case Some(o) =>
          context.self ! ClickReadSideActorOffset(o)
        case None =>
          context.self ! ClickReadSideActorOffset(NoOffset)
      }

      val ts = System.currentTimeMillis() - 1800000 - 200000
      val clicksStmt = new SimpleStatement(s"SELECT * FROM graph.clicks WHERE ts > $ts ALLOW FILTERING")

      val clicksQuery = CassandraSource(clicksStmt)
        .map { row =>
          NodeClickInfo(
            row.getString("company"),
            row.getString("id"),
            row.getMap("tags", classOf[String], classOf[java.lang.Integer]).asScala.toMap.keySet,
            row.getLong("ts"),
            row.getInt("clicks")
          )
        }
        .runWith(Sink.seq)

      clicksQuery.foreach {
        context.self ! OnStartNodeClickInfo(_)
      }

      def calculateOverallList(list: Seq[NodeClickInfo]): Map[String, Int] = {
        val unsortedOverallList: Map[String, Int] = list.foldLeft(Map.empty[String, Int]){
          case (listAcc, nodeClick) =>
            listAcc + (nodeClick.nodeId -> (listAcc.getOrElse(nodeClick.nodeId, 0) + nodeClick.clicks))
        }
        unsortedOverallList.toSeq.sortWith(_._2 > _._2).take(20).toMap
      }

      def collectClickStat(list: Seq[NodeClickInfo], sortedOverallList: Map[String, Int]): Behavior[ClickStatCommands] =
        Behaviors.receiveMessagePartial {
          case click: NodeClickInfo =>
            val newList = click +: (if(list.size > 1000) list.take(900) else list)

            val sortedOverallList: Map[String, Int] = calculateOverallList(newList)

            collectClickStat(newList, sortedOverallList)

          case TrendingNodesCommand(tags, replyTo) =>
//            val result = tags.foldLeft(Map.empty[String, Seq[(String, Int)]]){
//              case (acc, nodeType) =>
//                val unsortedNodeTypeList: Map[String, Int] = list.foldLeft(Map.empty[String, Int]){
//                  case (listAcc, nodeClick) if (nodeClick.tags & tags).nonEmpty =>
//                    listAcc + (nodeClick.nodeId -> (listAcc.getOrElse(nodeClick.nodeId, 0) + nodeClick.clicks))
//                  case (listAcc, _) =>
//                    listAcc
//                }
//                val sortedList = unsortedNodeTypeList.toSeq.sortWith(_._2 > _._2).take(5)
//                acc + (nodeType -> sortedList)
//            }
//
//            val unsortedOverallList: Map[String, Int] = list.foldLeft(Map.empty[String, Int]){
//              case (listAcc, nodeClick) =>
//                listAcc + (nodeClick.nodeId -> (listAcc.getOrElse(nodeClick.nodeId, 0) + nodeClick.clicks))
//            }

            replyTo ! TrendingNodesResponse(sortedOverallList, Map.empty)
            Behaviors.same
        }

      def waitingForInitialLoad: Behavior[ClickStatCommands] =
        Behaviors.receiveMessagePartial {
          case OnStartNodeClickInfo(list) =>
            context.log.debug(list.toString)
            context.log.debug(calculateOverallList(list).toString)
            buffer.unstashAll(collectClickStat(list, calculateOverallList(list)))
          case x =>
            buffer.stash(x)
            Behaviors.same
        }

      val waitingForOffset: Behavior[ClickStatCommands] =
        Behaviors.receiveMessagePartial {
          case ClickReadSideActorOffset(offset) =>
//            println(offset)
            val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(ClickUpdateEventName, offset)
            val stream: Future[Done] = createdStream
              .map {
                case ee@EventEnvelope(_, _, _, value: GraphNodeClickUpdated) =>
                  context.self ! NodeClickInfo(value.companyId, value.nodeId, value.tags.keySet, value.ts, value.clicks)
                  ee
                case ee =>
                  ee
              }
              .via(saveClickFlow)
              .via(offsetManagement.saveOffsetFlow(readSideConfig.producerParallelism, ClickUpdateEventName))
              .runWith(Sink.ignore) //TODO: for failures, move on to the next message

            buffer.unstashAll(waitingForInitialLoad)

          case x =>
            buffer.stash(x)
            Behaviors.same
        }

      waitingForOffset
    }
  }
}