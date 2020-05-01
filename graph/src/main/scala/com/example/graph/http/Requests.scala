package com.example.graph.http

import com.example.graph.GraphNodeEntity._

object Requests {
  case class CreateNodeReq(nodeId: String, nodeType: String, companyId: String, tags: Map[String, Int] = Map.empty, properties: Map[String, String] = Map.empty)
  case class UpdateNodeReq(properties: Map[String, String], tags: Map[String, Int] = Map.empty)
  case class RemoveEdgeReq(targetNodeId: String, edgeType: String)
  case class UpdateEdgeReq(targetNodeId: String, edgeType: String, direction: String = "to", userId: String, properties: EdgeProperties = Map.empty, userLabels: Map[String, Int] = Map.empty, weight: Int = 0)
  case class EdgeQueryReq(edgeType: EdgeType, properties: EdgeProperties)

  case class QueryReq(nodeType: Option[NodeType], edgeType: Set[EdgeType], nodeProperties: NodeProperties = Map.empty, edgeProperties: EdgeProperties = Map.empty)
  case class GraphQueryReq(queries: List[QueryReq])

  case class TrendingNodesReq(tags: Set[String])

  case class RelevantNodesReq(tags: Map[String, Int])
}
