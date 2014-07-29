# @Description: This class handles the interfacing between the scraping system and the redis server

fs    = require 'fs'
kson  = require 'kson'
Q     = require 'q'
redis = require 'redis'

class QueueInterface

  # @Description: Default constructor
  # @param: redisInfo:object
  #   - host:string
  #   - port:string
  #   - masterList:string
  #   - queueName:string
  #   - scrapeMode:string
  #       depth | breadth
  #         depth = go as deep as possible before going beadth
  #       breadth
  #         breadth = go as broad as possiblebefore going deep  
  # @param: initialCallBack:function()
  constructor: (redisInfo, initialCallBack)->
    @queue_names = ['QUARANTINED_TASKS']
    @eventListeners = {}
    
    #when true prevents pushing to queue stack
    @stop_send = false 
    
    #when true no longer listens for any messages
    @stop_receive = false
    
    @redisInfo = redisInfo
    @redisClient = redis.createClient redisInfo.port, redisInfo.host
    @redisEventListener = redis.createClient redisInfo.port, redisInfo.host
    
    # console.log "[QUEUE_INTERFACE] : Subscribed to : " + redisInfo.queueName + "*"
    @redisEventListener.psubscribe (redisInfo.queueName + "*")
      
    @redisEventListener.on 'pmessage', (channel_name , event_key, message)=>
      if @stop_receive then return
      event_name = event_key.split(':')[1]
      queueName = event_key.split(':')[0]
      try resObj = kson.parse message
      catch e then resObj = {}
      @eventListeners[event_name] && @eventListeners[event_name](queueName, resObj)

    initialCallBack?()

  # @Description: sets the event listeners, triggered with event is called remotely
  # @param: event_key:string
  # @param: callback:function()
  setEventListener: (event_key, callback)->
    @eventListeners[event_key] = callback

  # @Description: broadcast an event to all slaves and master in the cluster
  # @param: authToken:string
  # @param: eventName:string
  # @param: message:string
  # @param: callback:function()
  broadcast: (authToken, eventName, message, callback)->
    deferred = Q.defer()
    message = kson.stringify message
    switch eventName
      when 'mercy', 'status ping', 'new task', 'logs', 'results', 'kill task'
        @redisClient.publish authToken + ':' + eventName, message, (error, result)=>
          callback? true
          if error
            deferred.reject error
          else
            deferred.resolve true
      else
        callback? false
    deferred.promise        

  # @Description: Sets queueName to be Busy for x seconds
  # @param:   queueName:String
  # @param:   sec_expiry:Int
  # @param:   callback:function()
  # @return:  promise:Promise
  setIsBusy: (queueName, sec_expiry, callback)->
    deferred = Q.defer()
    @redisClient.setex "#{queueName}_BUSY", sec_expiry, "BUSY", (error, result)->
      callback? result
      if error
        deferred.reject error
      else
        deferred.resolve result

    deferred.promise


  # @Description: Atomic method to check that REDIS:queueName llen > 0 && REDIS:queueName_BUSY == true
  # @param: queueName:String
  # @param: callback:function( busy:Boolean )
  isBusy: (queueName, callback)->
    deferred = Q.defer()    
    @redisClient.multi([
      ["llen", queueName],
      ["get", "#{queueName}_BUSY"]
    ]).exec (err, replies)->
        is_busy = replies[0] > 0 || replies[1] == "BUSY"
        callback? is_busy
        if err
          deferred.reject err
        else
          deferred.resolve is_busy

    deferred.promise

  # @Description: check that REDIS:queueName_BUSY == true
  # @param: queueName:String
  # @param: callback:function( busy:Boolean )
  areEngaged: (queueName, callback)->
    deferred = Q.defer()
    @redisClient.get "#{queueName}_BUSY", (error, result)->
      are_engaged = result == "BUSY"
      callback? are_engaged
      if error
        deferred.reject error
      else
        deferred.resolve are_engaged
    deferred.promise

  # @Description: gets count of outstanding subtask for task
  # @param: queueName:string
  # @param: callback:function(result:integer)
  getNumTaskleft: (queueName, callback)->
    deferred = Q.defer()
    @queue_names.push(queueName) unless queueName in @queue_names
    @redisClient.llen queueName, (error, result)=>
      callback? result
      if error
        deferred.reject error
      else
        deferred.resolve result

    deferred.promise      

  # @Description: gets the next task from the queue
  # @param: queueName:string
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
  getTaskFromQueue: (queueName, callback)->
    deferred = Q.defer()
    @queue_names.push(queueName) unless queueName in @queue_names
    switch @redisInfo.scrapeMode
      when 'depth' then pop_method = 'rpop'
      when 'breadth' then pop_method = 'lpop'
      else pop_method = 'lpop'
      
    @redisClient[pop_method] queueName, (error, task_info_string)=>
      if error 
        deferred.reject error
      else
        try
          if task_info_string
            task_option_obj = kson.parse task_info_string
            callback? task_option_obj
            deferred.resolve task_option_obj
          else 
            callback? false
            deferred.resolve false
        catch error
          callback? false
          deferred.reject error
    deferred.promise

  # @Description: adds a new task to the end of queue
  # @param: queueName:string
  # @param: task_type:string
  #    - There are only 2 task_types to date :
  #       'listing page scrape'
  #       'detailed page scrape'  
  # @param: task_option_obj:object
  # @param: task_position:string
  # @param: callback:function()
  addTaskToQueue: (queueName, task_type, task_option_obj, task_position, callback)->
    deferred = Q.defer()
    @queue_names.push(queueName) unless queueName in @queue_names
    if @stop_send then return

    switch task_position
      when 'head' then pushMethod = 'lpush'
      when 'tail' then pushMethod = 'rpush'
      when 'bad' then pushMethod = 'rpush'
      else pushMethod = 'rpush'

    if task_position == 'bad' then queueName == 'QUARANTINED_TASKS'

    task_option_obj.task_id = queueName
    task_option_obj.task_type = task_type
    task_info_string = kson.stringify task_option_obj
    @redisClient[pushMethod] queueName, task_info_string, (error, result)=>
      callback? result
      if error
        deferred.reject error
      else
        deferred.resolve result

    deferred.promise


  
  # @Description: empties the task queue
  # @param: queueName:string
  # @param: callback:function()  
  emptyQueue : (queueName, callback)->
    deferred = Q.defer()    
    @redisClient.del queueName, (error, result)=>
      callback? result
      if error
        deferred.reject error
      else
        deferred.resolve result

    deferred.promise

  # Returns the environment this queue is currently running in
  environment : ()->
    process.env['NODE_ENV']

  # Empties the redis database of all jobs
  clear : (callback)->
    @redisClient.flushall (err, succeeded)=>
      callback? err, succeeded

  # Shuts down all redis clients listened by this Krake and empties the queue
  # This method is only available in testing mode
  quit : (callback)->
    @redisClient.flushall (err, succeeded)=>
      @redisClient.quit()
      @redisEventListener.quit()
      callback? err, succeeded
  


module.exports = QueueInterface