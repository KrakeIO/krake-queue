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
  #   - authToken:string
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
    @redisEventListener.psubscribe (redisInfo.authToken + "*")
      
    @redisEventListener.on 'pmessage', (event_key, message)=>
      if @stop_receive then return
      event_name = event_key.split(':')[1]
      @processEvent(event_name, message)

    initialCallBack && initialCallBack()
  
  
  
  # @Description: triggers the callback function to be called for the incoming event
  # @param event_key:string
  processEvent: (event_key, message)->
    try
      resObj = kson.parse message
      
    catch e
      resObj = {}
      
    @eventListeners[event_key] && @eventListeners[event_key](resObj)


  
  # @Description: sets the event listeners, triggered with event is called remotely
  # @param: event_key:string
  # @param: callback:function()
  setEventListener: (event_key, callback)->
    @eventListeners[event_key] = callback



  broadcast: (eventName, message, callback)->
    switch eventName
      when 'mercy', 'status ping', 'new task', 'logs', 'results', 'kill task'
        @redisClient.publish @redisInfo.authToken + ':' + eventName, message, (error, result)=>
          callback && callback()
      else
        console.log '[QUEUE_INTERFACE] unrecognized event : %s ', eventName

  
  # @Description: notifies all slaves to respond with their status
  # @param: beg_for_mercy:object
  statusPingResponse: (beg_for_mercy)->
    @broadcast beg_for_mercy



  # @Description: notifies all slaves to respond with their status
  statusPing: ()->
    @broadcast 'status ping'


  # @Description: notifies all slave there is a new task on the tray
  announceNewTask: (auth_token)->
    @broadcast 'new task', auth_token

  
  # @Description: returns the logs to the master
  # @param: logs_channel_key:string
  # @param: logs_obj:Object
  returnLogs: (logs_channel_key, logs_obj)->
    logs_string = kson.stringify logs_obj
    @broadcast 'logs', logs_string


  
  # @Description: returns the results to the master
  # @param: results_channel_key:string
  # @param: results_obj:Object
  returnResults: (results_channel_key, results_obj)->
    results_string = kson.stringify results_obj
    @broadcast 'results', results_string



  # @Description: gets count of outstanding subtask for task
  # @param: callback:function(result:integer)
  getNumTaskleft: (callback)->
    @redisClient.llen @redisInfo.authToken, (error, result)=>
      callback result



  # @Description: clears all the tasks in the cluster
  #   A cluster will include the slave instances as well as the queue in the Redis Server
  # @param: callback:function  
  abortTask: (callback)->
      
    @boardcast 'kill task', (error, result)=>
      console.log '[QUEUE_INTERFACE] : Genocide committed on all Krakes in cluster'
      @emptyQueue callback


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

    @redisClient[pop_method] @redisInfo.authToken, (error, task_info_string)=>
      try
        if task_info_string
          task_option_obj = kson.parse task_info_string
          callback task_option_obj
        else 
          callback false          
      catch error
        callback false



  # @Description: adds a new task to the end of queue
  # @param: auth_token:string
  # @param: task_type:string
  #    - There are only 2 task_types to date :
  #       'listing page scrape'
  #       'detailed page scrape'  
  # @param: task_option_obj:object
  # @param: task_position:string
  # @param: callback:function()
  addTaskToQueue: (auth_token, task_type, task_option_obj, task_position, callback)->

    if @stop_send
      console.log '[QUEUE_INTERFACE] : Embargoed. Not pushing anything to the queue stack'
      return

    switch task_position
      when 'head' then pushMethod = 'lpush'
      when 'tail' then pushMethod = 'rpush'
      when 'bad' then pushMethod = 'rpush'  
      else pushMethod = 'rpush'

    if queueName == 'QUARANTINED_TASKS'
    else queueName = @redisInfo.authToken

    task_option_obj.task_id = auth_token
    task_option_obj.task_type = task_type
    task_info_string = kson.stringify task_option_obj
    @redisClient[pushMethod] queueName, task_info_string, (error, result)=>
      @announceNewTask(auth_token)
      callback && callback()


  
  # @Description: empties the task queue
  emptyQueue : (callback)->
    @redisClient.del @redisInfo.authToken, (error, result)=>
      console.log '[QUEUE_INTERFACE] : Task Queue (%s) was emptied', @redisInfo.authToken        
      callback && callback()
  


module.exports = QueueInterface