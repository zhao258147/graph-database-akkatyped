package com.example.graph.http

import com.example.graph.GraphNodeEntity._

object Requests {
  case class CreateNodeReq(nodeId: String, nodeType: String, properties: Map[String, String] = Map.empty)
  case class UpdateNodeReq(properties: Map[String, String])
  case class RemoveEdgeReq(targetNodeId: String, edgeType: String)
  case class UpdateEdgeReq(targetNodeId: String, edgeType: String, direction: String = "to", properties: EdgeProperties = Map.empty)
  case class EdgeQueryReq(edgeType: EdgeType, properties: EdgeProperties)

  case class QueryReq(nodeType: Option[NodeType], edgeType: Option[EdgeType], nodeProperties: NodeProperties = Map.empty, edgeProperties: EdgeProperties = Map.empty)
  case class GraphQueryReq(queries: List[QueryReq])
}
