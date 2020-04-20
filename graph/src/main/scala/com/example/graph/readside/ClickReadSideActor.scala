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

import scala.concurrent.{ExecutionContextExecutor, Future}

object ClickReadSideActor {
  val ClickUpdateEventName = "clickupdate"

  sealed trait ClickStatCommands

  case class ClickReadSideActorOffset(offset: Offset) extends ClickStatCommands

  case class NodeClickInfo(nodeId: String, nodeType: String, ts: Long, clicks: Int) extends ClickStatCommands

  case class OnStartNodeClickInfo(list: Seq[NodeClickInfo]) extends ClickStatCommands
  case class TrendingNodesCommand(types: List[String], replyTo: ActorRef[TrendingNodesResponse]) extends ClickStatCommands

  case class TrendingNodesResponse(overallRanking: Seq[String], rankingByType: Map[String, Seq[String]])

  def behaviour(
    readSideConfig: ReadSideConfig,
    offsetManagement: OffsetManagement
  )(implicit session: Session): Behavior[ClickStatCommands] = Behaviors.withStash(100) { buffer =>
    Behaviors.setup[ClickStatCommands] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      val clickInsertStatement = session.prepare(s"INSERT INTO graph.clicks(type, id, ts, clicks) VALUES (?, ?, ?, ?)")
      val clickInsertBinder =
        (e: EventEnvelope, statement: PreparedStatement) => {
          e.event match {
            case elemToInsert: GraphNodeClickUpdated =>
              println(elemToInsert)
              statement.bind(
                elemToInsert.nodeType,
                elemToInsert.nodeId,
                new java.lang.Long(elemToInsert.ts),
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

      val offsetStmt = new SimpleStatement(s"SELECT * FROM graph.read_side_offsets WHERE tag = '$ClickUpdateEventName'").setFetchSize(1)

      val offsetQuery = CassandraSource(offsetStmt)
        .map(row => Offset.timeBasedUUID(row.getUUID("offset")))
        .runWith(Sink.seq).map(_.headOption)

      offsetQuery.foreach {
        case Some(o) =>
          println(o)
          context.self ! ClickReadSideActorOffset(o)
        case None =>
          context.self ! ClickReadSideActorOffset(NoOffset)
      }

      val ts = System.currentTimeMillis() - 1800000 - 200000
      val clicksStmt = new SimpleStatement(s"SELECT * FROM graph.clicks WHERE ts > $ts ALLOW FILTERING")

      val clicksQuery = CassandraSource(clicksStmt)
        .map { row =>
          NodeClickInfo(
            row.getString("id"),
            row.getString("type"),
            row.getLong("ts"),
            row.getInt("clicks")
          )
        }
        .runWith(Sink.seq)

      clicksQuery.foreach {
        context.self ! OnStartNodeClickInfo(_)
      }

      def collectClickStat(list: Seq[NodeClickInfo]): Behavior[ClickStatCommands] =
        Behaviors.receiveMessagePartial {
          case click: NodeClickInfo =>
            val newList = if(list.size > 1000) list.take(900) else list

            collectClickStat(click +: newList)

          case TrendingNodesCommand(types, replyTo) =>
            val result = types.foldLeft(Map.empty[String, Seq[String]]){
              case (acc, nodeType) =>
                val unsortedNodeTypeList: Map[String, Int] = list.foldLeft(Map.empty[String, Int]){
                  case (listAcc, nodeClick) if nodeClick.nodeType == nodeType =>
                    listAcc + (nodeClick.nodeId -> listAcc.getOrElse(nodeClick.nodeId, nodeClick.clicks))
                  case (listAcc, _) =>
                    listAcc
                }
                val sortedList = unsortedNodeTypeList.toSeq.sortWith(_._2 > _._2).map(_._1).take(5)
                acc + (nodeType -> sortedList)
            }

            val unsortedOverallList: Map[String, Int] = list.foldLeft(Map.empty[String, Int]){
              case (listAcc, nodeClick) =>
                listAcc + (nodeClick.nodeId -> listAcc.getOrElse(nodeClick.nodeId, nodeClick.clicks))
            }
            val sortedOverallList = unsortedOverallList.toSeq.sortWith(_._2 > _._2).map(_._1).take(5)

            println("TrendingNodesResponse " * 10)
            println(list)
            println(result)
            replyTo ! TrendingNodesResponse(sortedOverallList, result)
            Behaviors.same
        }

      def waitingForInitialLoad: Behavior[ClickStatCommands] =
        Behaviors.receiveMessagePartial {
          case OnStartNodeClickInfo(list) =>
            println("OnStartNodeClickInfo " * 10)
            println(list)
            buffer.unstashAll(collectClickStat(list))
          case x =>
            buffer.stash(x)
            Behaviors.same
        }

      val waitingForOffset: Behavior[ClickStatCommands] =
        Behaviors.receiveMessagePartial {
          case ClickReadSideActorOffset(offset) =>
            println("click readside" * 10)
            val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(ClickUpdateEventName, offset)
            val stream: Future[Done] = createdStream
              .map {
                case ee@EventEnvelope(_, _, _, value: GraphNodeClickUpdated) =>
                  context.self ! NodeClickInfo(value.nodeId, value.nodeType, value.ts, value.clicks)
                  ee
                case ee =>
                  ee
              }
              .via(saveClickFlow)
              .via(offsetManagement.saveOffsetFlow(readSideConfig.producerParallelism, ClickUpdateEventName))
              .runWith(Sink.foreach(println)) //TODO: for failures, move on to the next message

            buffer.unstashAll(waitingForInitialLoad)

          case x =>
            buffer.stash(x)
            Behaviors.same
        }

      waitingForOffset
    }
  }
}