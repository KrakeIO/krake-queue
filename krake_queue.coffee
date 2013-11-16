kson = require 'kson'
# @Description: This class handles the interfacing between the scraping system and the redis server
redis = require 'redis'
fs = require 'fs'

class QueueInterface

  # @Description: Default constructor
  # @param: redisInfo:object
  #   - host:string
  #   - port:string
  #   - masterList:string
  #   - slaveList:string
  #   - taskQueue:string
  #   - eventEnqueue:string
  #   - scrapeMode:string
  #       depth | breadth
  #         depth = go as deep as possible before going beadth
  #       breadth
  #         breadth = go as broad as possiblebefore going deep  
  # @param: initialCallBack:function()
  constructor: (redisInfo, initialCallBack)->
    @eventListeners = {}
    
    #when true prevents pushing to queue stack
    @stop_send = false 
    
    #when true no longer listens for any messages
    @stop_receive = false
    
    @redisInfo = redisInfo
    @redisClient = redis.createClient redisInfo.port, redisInfo.host
    @redisEventListener = redis.createClient redisInfo.port, redisInfo.host
    @redisEventListener.subscribe redisInfo.eventEnqueue
    @redisEventListener.on 'message', (queue_name, event_key)=>
      if !@stop_receive && queue_name == redisInfo.eventEnqueue
        @processEvent(event_key)

    initialCallBack && initialCallBack()
  
  
  
  # @Description: triggers the callback function to be called for the incoming event
  # @param event_key:string
  processEvent: (event_key)->
    @eventListeners[event_key] && @eventListeners[event_key]()


  
  # @Description: sets the event listeners, triggered with event is called remotely
  # @param: event_key:string
  # @param: callback:function()
  setEventListener: (event_key, callback)->
    @eventListeners[event_key] = callback


  
  # @Description: gets the next task from the queue
  # @param: callback:function( task_option_obj:object || false:boolean )
  #    E.g. task_option_obj
  #      options = 
  #        origin_url : 'http://www.mdscollections.com/cat_mds_accessories17.cfm'
  #        columns : [{
  #            col_name : 'title'
  #            dom_query : '.listing_product_name' 
  #          }, { 
  #            col_name : 'price'
  #            dom_query : '.listing_price' 
  #           }, { 
  #            col_name : 'detailed_page_href'
  #            dom_query : '.listing_product_name'
  #            required_attribute : 'href'
  #         }]
  #        next_page :
  #          dom_query : '.listing_next_page'
  #        detailed_page :
  #          columns : [{
  #            col_name : 'description'
  #            dom_query : '.tabfield18504'
  #          }]   
  getTaskFromQueue: (callback)->

    switch @redisInfo.scrapeMode
      when 'depth' then pop_method = 'rpop'
      when 'breadth' then pop_method = 'lpop'
      else pop_method = 'lpop'
      
    @redisClient[pop_method] @redisInfo.taskQueue, (error, task_info_string)=>
      try
        if task_info_string
          task_option_obj = kson.parse task_info_string
          callback task_option_obj
        else 
          callback false          
      catch error
        callback false


  
  # @Description: adds a new task to the end of queue
  # @param: master_id:string
  # @param: task_id:string
  # @param: task_type:string
  #    - There are only 2 task_types to date :
  #       'listing page scrape'
  #       'detailed page scrape'  
  # @param: task_option_obj:object
  # @param: callback:function()
  addTaskToQueue: (master_id, task_id, task_type, task_option_obj, task_position, callback)->
  
    if @stop_send
      console.log '[QUEUE_INTERFACE] : Embargoed. Not pushing anything to the queue stack'
      return

    switch task_position
      when 'head' then pushMethod = 'lpush'
      when 'tail' then pushMethod = 'rpush'
      when 'bad' then pushMethod = 'rpush'  
      else pushMethod = 'rpush'
    
    if queueName == 'QUARANTINED_TASKS'
    else queueName = @redisInfo.taskQueue
    
    task_option_obj.master_id = master_id
    task_option_obj.task_id = task_id
    task_option_obj.task_type = task_type
    task_info_string = kson.stringify task_option_obj
    @redisClient[pushMethod] queueName, task_info_string, (error, result)=>
      @announceNewTask()
      callback && callback()



  # @Description: notifies all slaves to respond with their status
  # @param: beg_for_mercy:object
  statusPingResponse: (beg_for_mercy)->
    @redisClient.publish @redisInfo.eventEnqueue, beg_for_mercy, (error, result)=>
      #console.log '[RETURNED STATUS OF SLAVE] %s, %s', error, result



  # @Description: notifies all slaves to respond with their status
  statusPing: ()->
    @redisClient.publish @redisInfo.eventEnqueue, 'status ping', (error, result)=>
      #console.log '[CHECKING STATUS OF SLAVES] %s, %s', error, result



  # @Description: notifies all slave there is a new task on the tray
  announceNewTask: ()->
    @redisClient.publish @redisInfo.eventEnqueue, 'new task', (error, result)=>
      #console.log '[NEW TASK ANNOUNCEMENT] %s, %s', error, result


  
  # @Description: gets the next master_id to be assigned
  # @param: callback:function(new_id:string)
  getNewMasterId: (callback)->
    @redisClient.llen @redisInfo.masterList, (error, num)=>
      new_id = 'MASTER_' + num
      @redisClient.lpush @redisInfo.masterList, new_id, (error, result)=>
        console.log '[QUEUE_INTERFACE] : Length of master list %s', new_id
        callback new_id



  # @Description: gets the channel to publish and listen for logs for a particular task
  # @param: master_id:string
  # @param: task_id:string
  # @return: results_channel_key:string
  getLogsChannelKey: (master_id, task_id)->
    master_id + '_' + task_id + '_logs'



  # @Description: sets up a channel to the master object for slaves to return logs to
  # @param: master_id_task_id_log:string
  # @param: callback:function(log_obj:Object || false:boolean)
  createLogsChannel: (master_id_task_id_log, callback)->
    console.log '[QUEUE_INTERFACE] : creating channel %s', master_id_task_id_log
    @redisLogsListener = redis.createClient @redisInfo.port, @redisInfo.host
    @redisLogsListener.subscribe master_id_task_id_log
    @redisLogsListener.on 'message', (master_id_task_id_log, logs_string)=>
      if @stop_receive
        return
            
      try 
        logs_obj = kson.parse logs_string
        callback && callback logs_obj
      catch error
        error_msg = 'Queue interface: [ERROR] ' + error + ', ' + 
          '\r\n=========================\r\n' +
          logs_string + 
          '\r\n=========================\r\n'
        logs_obj = {}
        logs_obj.type = 'error'
        logs_obj.message = error_msg
        console.log error_msg        


  
  # @Description: returns the logs to the master
  # @param: logs_channel_key:string
  # @param: logs_obj:Object
  returnLogs: (logs_channel_key, logs_obj)->
    logs_string = kson.stringify logs_obj
    @redisClient.publish logs_channel_key, logs_string, (error, result)=>
      # console.log '[LOGS RETURNED] %s, %s', error, result  


  
  # @Description: gets the channel to publish and listen for results to a particular task
  # @param: master_id:string
  # @param: task_id:string
  # @return: results_channel_key:string
  getResultsChannelKey: (master_id, task_id)->
    master_id + '_' + task_id + '_results'  


  
  # @Description: sets up a channel to the master object for slaves to return results to
  # @param: master_id_task_id_result:string
  # @param: callback:function(results_obj:Object || false:boolean)
  createResultsChannel: (master_id_task_id_result, callback)->
    console.log '[QUEUE_INTERFACE] : creating channel %s', master_id_task_id_result
    @redisResultsListener = redis.createClient @redisInfo.port, @redisInfo.host
    @redisResultsListener.subscribe master_id_task_id_result
    @redisResultsListener.on 'message', (master_id_task_id_result, results_string)=>
      if @stop_receive
        return
        
      try 
        results_obj = kson.parse results_string
        callback results_obj
      catch error
        console.log 'Interface: ', error
        callback false


  
  # @Description: returns the results to the master
  # @param: results_channel_key:string
  # @param: results_obj:Object
  returnResults: (results_channel_key, results_obj)->
    results_string = kson.stringify results_obj
    @redisClient.publish results_channel_key, results_string, (error, result)=>
      # console.log '[RESULTS RETURNED] %s, %s', error, result  


              
  # @Description: gets count of outstanding subtask for task
  # @param: callback:function(result:integer)
  getNumTaskleft: (callback)->
    @redisClient.llen @redisInfo.taskQueue, (error, result)=>
      callback result



  # @Description: clears all the tasks in the cluster
  #   A cluster will include the slave instances as well as the queue in the Redis Server
  # @param: callback:function  
  abortTask: (callback)->
      
    # Informs all slaves in the cluster to kill off the current task they are processing
    @redisClient.publish @redisInfo.eventEnqueue, 'kill task', (error, result)=>
      console.log '[QUEUE_INTERFACE] : Genocide committed on all Krakes in cluster'
      # Clears all the task in a task queue
      @redisClient.del @redisInfo.taskQueue, (error, result)=>
        console.log '[QUEUE_INTERFACE] : Task Queue (%s) was emptied', @redisInfo.taskQueue        
        callback && callback()

module.exports = QueueInterface
  