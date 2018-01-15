defmodule Project2 do
  use GenServer
  @moduledoc """
  Documentation for Project2.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Project2.hello
      :world

  """
  


  def main(args) do
    if (Enum.count(args)!=3) do
      IO.puts" Illegal Arguments Provided"
      System.halt(1)
    else
        numNodes=Enum.at(args, 0)|>String.to_integer()
      
        topology=Enum.at(args, 1)
        algorithm=Enum.at(args, 2)

        if topology == "2D" || topology == "imp2D" do
          numNodes = getNextPerfectSq(numNodes)
        end
        allNodes = Enum.map((1..numNodes), fn(x) ->
          pid=start_node()
          updatePIDState(pid, x)
          pid
        end)

        table = :ets.new(:table, [:named_table,:public])
        :ets.insert(table, {"count",0})
     
        buildTopology(topology,allNodes)
        startTime = System.monotonic_time(:millisecond)
      
        startAlgorithm(algorithm, allNodes, startTime)
        infiniteLoop()
    end
  end 

  def infiniteLoop() do
    infiniteLoop()
  end

  def checkEtsTable(numNodes, startTime,table, parent) do
    
    [{_, currentCount}] = :ets.lookup(table, "count")

    if currentCount == (0.9*numNodes) do
      currentTime = System.system_time(:millisecond)
      endTime = currentTime - startTime
      IO.puts "Convergence Achieved in = "<> Integer.to_string(endTime)
      Process.exit(parent, :kill)
    end
    checkEtsTable(numNodes,startTime, table, parent)
  end
  def buildTopology(topology,allNodes) do
    case topology do
      "full" ->buildFull(allNodes)
      "2D" ->build2D(allNodes)
      "line" ->buildLine(allNodes)
      "imp2D" ->buildImp2D(allNodes)
    end
  end

  def buildFull(allNodes) do
    Enum.each(allNodes, fn(k) ->
      adjList=List.delete(allNodes,k) 
      updateAdjacentListState(k,adjList)
    end)
  end


  def getNextPerfectSq(numNodes) do
    round :math.pow(:math.ceil(:math.sqrt(numNodes)) ,2)
  end

  def build2D(allNodes) do
    numNodes=Enum.count allNodes
    numNodesSQR= :math.sqrt numNodes
    Enum.each(allNodes, fn(k) ->
      adjList=[]
      count=Enum.find_index(allNodes, fn(x) -> x==k end)

      if(!isNodeBottom(count,numNodes)) do
        index=count + round(numNodesSQR)
        neighbhour1=Enum.fetch!(allNodes, index)
        adjList = adjList ++ [neighbhour1]
      end
      
      if(!isNodeTop(count,numNodes)) do
        index=count - round(numNodesSQR)
        neighbhour2=Enum.fetch!(allNodes, index)
        adjList = adjList ++ [neighbhour2]
      end

      if(!isNodeLeft(count,numNodes)) do
        index=count - 1
        neighbhour3=Enum.fetch!(allNodes,index )
        adjList = adjList ++ [neighbhour3]
       end
      
      if(!isNodeRight(count,numNodes)) do
        index=count + 1
        neighbhour4=Enum.fetch!(allNodes, index)
        adjList = adjList ++ [neighbhour4]
      end
      updateAdjacentListState(k,adjList)
    end)
  end

  def buildLine(allNodes) do

    numNodes=Enum.count allNodes
    Enum.each(allNodes, fn(k) ->
      count=Enum.find_index(allNodes, fn(x) -> x==k end)

      cond do
        numNodes==count+1 ->
          neighbhour1=Enum.fetch!(allNodes, count - 1)
          neighbhour2=List.first (allNodes)
        true ->
          neighbhour1=Enum.fetch!(allNodes, count + 1)
          neighbhour2=Enum.fetch!(allNodes, count - 1)
      end
      adjList=[neighbhour1,neighbhour2]
      updateAdjacentListState(k,adjList)
    end)
  end

  def buildImp2D (allNodes) do

    numNodes=Enum.count allNodes
    numNodesSQR = :math.sqrt numNodes
    Enum.each(allNodes, fn(k) ->
      adjList=[]
      tempList=allNodes
      count=Enum.find_index(allNodes, fn(x) -> x==k end)
      if(!isNodeBottom(count,numNodes)) do
        index=count + round(numNodesSQR)
        neighbhour1=Enum.fetch!(allNodes, index)
        adjList = adjList ++ [neighbhour1]
        tempList=List.delete_at(tempList, index)
      end
      
      if(!isNodeTop(count,numNodes)) do
        index=count - round(numNodesSQR)
        neighbhour2=Enum.fetch!(allNodes, index)
        adjList = adjList ++ [neighbhour2]
        tempList=List.delete_at(tempList, index)
      end

      if(!isNodeLeft(count,numNodes)) do
        neighbhour3=Enum.fetch!(allNodes, count - 1)
        adjList = adjList ++ [neighbhour3]
        tempList=List.delete_at(tempList, count - 1)
      end
      
      if(!isNodeRight(count,numNodes)) do
        neighbhour4=Enum.fetch!(allNodes, count + 1)
        adjList = adjList ++ [neighbhour4]
        tempList=List.delete_at(tempList, count + 1)
      end
      
      neighbhour5=Enum.random(tempList)
      adjList = adjList ++ [neighbhour5]   
      updateAdjacentListState(k,adjList)
    end)
  end


  def isNodeBottom(i,length) do
    if(i>=(length-(:math.sqrt length))) do
      true
    else
      false
    end
  end
  
  def isNodeTop(i,length) do
    if(i< :math.sqrt length) do
      true
    else
      false
    end
  end

  def isNodeLeft(i,length) do
    if(rem(i,round(:math.sqrt(length))) == 0) do
      true
    else
      false
    end
  end

  def isNodeRight(i,length) do
    if(rem(i + 1,round(:math.sqrt(length))) == 0) do
      true
    else
      false
    end
  end

  def startAlgorithm(algorithm,allNodes, startTime) do
    case algorithm do
      "gossip" -> startGossip(allNodes, startTime)
      "push-sum" ->startPushSum(allNodes, startTime)
    end
  end

  def startGossip(allNodes, startTime) do
    chosenFirstNode = Enum.random(allNodes)
    updateCountState(chosenFirstNode, startTime, length(allNodes))
    recurseGossip(chosenFirstNode, startTime, length(allNodes))

  end

  def recurseGossip(chosenRandomNode, startTime, total) do
    
    myCount = getCountState(chosenRandomNode)
   
    cond do
      myCount < 11 ->
        adjacentList = getAdjacentList(chosenRandomNode)
        chosenRandomAdjacent=Enum.random(adjacentList)
        Task.start(Project2,:receiveMessage,[chosenRandomAdjacent, startTime, total])
        recurseGossip(chosenRandomNode, startTime, total)
      true -> 
        Process.exit(chosenRandomNode, :normal)
    end
      recurseGossip(chosenRandomNode, startTime, total)
  end

  def startPushSum(allNodes, startTime) do
    chosenFirstNode = Enum.random(allNodes)
    GenServer.cast(chosenFirstNode, {:ReceivePushSum,0,0,startTime, length(allNodes)})
  end
  
  def handle_cast({:ReceivePushSum,incomingS,incomingW,startTime, total_nodes},state) do

    {s,pscount,adjList,w} = state

    myS = s + incomingS
    myW = w + incomingW

    difference = abs((myS/myW) - (s/w))

    if(difference < :math.pow(10,-10) && pscount==2) do
      count = :ets.update_counter(:table, "count", {2,1})
      if count == total_nodes do
        endTime = System.monotonic_time(:millisecond) - startTime
        IO.puts "Convergence achieved in = " <> Integer.to_string(endTime) <>" Milliseconds"
        System.halt(1)
      end
    end

    if(difference < :math.pow(10,-10) && pscount<2) do
      pscount = pscount + 1 
    end

    if(difference > :math.pow(10,-10)) do
      pscount = 0
    end
    state = {myS/2,pscount,adjList,myW/2}

    randomNode = Enum.random(adjList)
    sendPushSum(randomNode, myS/2, myW/2,startTime, total_nodes)
    {:noreply,state}
  end

  def sendPushSum(randomNode, myS, myW,startTime, total_nodes) do
    GenServer.cast(randomNode, {:ReceivePushSum,myS,myW,startTime, total_nodes})
  end

  def updatePIDState(pid,nodeID) do 
    GenServer.call(pid, {:UpdatePIDState,nodeID})
  end

  def handle_call({:UpdatePIDState,nodeID}, _from ,state) do 
    {a,b,c,d} = state
    state={nodeID,b,c,d}
    {:reply,a, state} 
  end
  
  def updateAdjacentListState(pid,map) do 
    GenServer.call(pid, {:UpdateAdjacentState,map})
  end

  def handle_call({:UpdateAdjacentState,map}, _from, state) do 
    {a,b,c,d}=state
    state={a,b,map,d}
    {:reply,a, state} 
  end 

  def updateCountState(pid, startTime, total) do 
   
      GenServer.call(pid, {:UpdateCountState,startTime, total})
    
  end

  def handle_call({:UpdateCountState,startTime, total}, _from,state) do 
    {a,b,c,d}=state
    if(b==0) do
      count = :ets.update_counter(:table, "count", {2,1})
      if(count == total) do
        endTime = System.monotonic_time(:millisecond) - startTime
        IO.puts "Convergence achieved in = #{endTime} Milliseconds"
        System.halt(1)
      end
    end
    state={a,b+1,c,d}
    {:reply, b+1, state} 
  end


  def getCountState(pid) do 
    GenServer.call(pid,{:GetCountState})
  end

  def handle_call({:GetCountState}, _from ,state) do 
    {a,b,c,d}=state
    {:reply,b, state} 
  end

  def receiveMessage(pid, startTime, total) do
    updateCountState(pid, startTime, total)
    recurseGossip(pid, startTime, total)
  end 

  def getAdjacentList(pid) do 
    GenServer.call(pid,{:GetAdjacentList})
  end

  def handle_call({:GetAdjacentList}, _from ,state) do 
    {a,b,c,d}=state
    {:reply,c, state} 
  end

  def init(:ok) do
    {:ok, {0,0,[],1}} #{s,pscount,adjList,w} , {nodeId,count,adjList,w}
  end

  def start_node() do
    {:ok,pid}=GenServer.start_link(__MODULE__, :ok,[])
    pid
  end
 
end
