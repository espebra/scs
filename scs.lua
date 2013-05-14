local common = require "scs.common"
local timer = require "scs.timer"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

local function bucket_index(r)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg
    local internal = r['internal']
    local bucket = r['bucket']

    local max_keys = r['max-keys']
    if not max_keys then
        max_keys = 1000
    end
    local prefix = r['prefix']
    local marker = r['marker']

    if internal then
        -- Return a table with the objects on this host
        local objects = common.scandir(bucket)
        if objects then
            ngx.log(ngx.ERR,"Found " .. #objects .. " in the internal request")
        end

        local json = cjson.encode(objects)
        if json then
            ngx.print(json)
            exitcode = ngx.HTTP_OK
        end
    else
        -- Query the replica hosts for a table of objects
        local conf = common.get_configuration()
        local method = "GET"
        local timeout = 10000
        local path = "/?bucket=" .. bucket .. "&max-keys=" .. max_keys
        local headers = {}
        headers['user-agent'] = "scs internal"

        local objects = {}
        for host,h in pairs(conf.current.hosts) do
            if common.get_host_status(host) then
                local port = common.get_bind_port()
                local status, body = common.http_request(host, port, headers, method, path, timeout)
                if status then
                    ngx.log(ngx.INFO,"Object list retrieved successfully from " .. host)
                    local host_objects = cjson.decode(body)
                    if host_objects then
                        for host_object,v in pairs(host_objects) do
                            if not objects[host_object] then
                                v['replicas'] = 1
                                objects[host_object] = v
                            else
                                objects[host_object]['replicas'] = objects[host_object]['replicas'] + 1
                                if objects[host_object]['mtime'] < v['mtime'] then
                                    objects[host_object]['mtime'] = v['mtime']
                                    objects[host_object]['LastModified'] = v['LastModified']
                                end
                            end
                        end
                    end
                else
                    ngx.log(ngx.WARN,"Failed to retrieve the object list from " .. host)
                end
            end
        end

        local res = {}
        res['bucket'] = bucket
        if prefix then
            res['prefix'] = prefix
        end
        if marker then
            res['marker'] = marker
        end
        res['contents'] = objects

        -- for i,o in pairs(objects) do
        -- end

        local json = cjson.encode(res)
        if json then
            ngx.print(json)
            exitcode = ngx.HTTP_OK
        end
    end
    return exitcode
end

local function lookup_object(r)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg
    -- See if the object exists locally
    local object = r['object']
    local bucket = r['bucket']
    local object_base64 = r['object_base64']
    local internal = r['internal']
    local dir = common.get_storage_directory()

    -- The object do not exist locally
    if not internal then
        -- We do not have the file locally. Should lookup the hash table to
        -- find a valid host to redirect to. 302.
        local sites = common.get_object_replica_sites(bucket, object)
        local hosts = common.get_replica_hosts(bucket, object, sites)

        -- Easier to understand what is happening when debugging
        local hosts_text = "["
        for _,host in pairs(hosts) do
            hosts_text = hosts_text .. " " .. host
        end
        hosts_text = hosts_text .. " ]"

        local host = common.get_host_with_object(hosts, bucket, object)
        if host == nil then
            msg = "All the replica hosts for object " .. object .. " in bucket " .. bucket .. " are unavailable. Please try again later " .. hosts_text
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        elseif host == false then
            msg = "The object " .. object .. " in bucket " .. bucket .. " does not exist locally or on any of the available replica hosts " .. hosts_text
            exitcode = ngx.HTTP_NOT_FOUND
        else
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object,bucket)
            msg = 'Redirecting GET request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
            ngx.header["Location"] = url
            exitcode = ngx.HTTP_MOVED_TEMPORARILY
        end
    end
    return exitcode, msg
end

local function post_object(r)
    local internal = r['internal']
    local bucket = r['bucket']
    local object = r['object']

    local sites = common.get_object_replica_sites(bucket, object)
    local hosts = common.get_replica_hosts(bucket, object, sites)
    local exitcode = ngx.HTTP_NOT_FOUND

    local dir = common.get_storage_directory()
    if common.object_fits_on_this_host(hosts) then
        local object_base64 = ngx.encode_base64(object)
        local path = dir .. "/" .. bucket .. "/" .. r['dir']
        if not os.rename(path, path) then
            os.execute('mkdir -p ' .. path)
        end

        ngx.req.read_body()
        local req_body_file = ngx.req.get_body_file()

        if not req_body_file then
            ngx.log(ngx.ERR,'No file found in request')
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        if req_body_file == nil then
            ngx.log(ngx.ERR,'Request body is nil')
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        tmpfile = io.open(req_body_file)
        realfile = io.open(path .. "/" .. object_base64, 'w')

        local size = 2^20      -- good buffer size (1M)
        while true do
            local block = tmpfile:read(size)
            if not block then 
                break
            end
            realfile:write(block)
        end
        tmpfile:close()
        realfile:close()

        if common.is_file(dir .. "/" .. bucket .. "/" .. r['dir'] .. "/" .. object_base64) then
            ngx.log(ngx.INFO,'The object ' .. object .. ' in bucket ' .. bucket .. ' was written successfully to local file system.')
            exitcode = ngx.HTTP_OK
        else
            ngx.log(ngx.ERR,'Failed to write object ' .. object .. ' in bucket ' .. bucket .. ' to local file system')
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        end

        -- Finish the request here if the configuration is set to write back.
        local write_back = common.get_write_back()
        if write_back then
            ngx.eof()
        end

        -- Replicate the object to other hosts here.
        for _,host in pairs(hosts) do
            if common.get_host_status(host) then
                local res = common.sync_object(dir, r['dir'], host, bucket, object_base64)
                if res then
                    ngx.log(ngx.NOTICE,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " succeeded.")
                else
                    ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " failed.")
                end
            else
                ngx.log(ngx.WARN,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " not initiated. The host is down.")
            end
        end
    else
        local host = nil
        local hosts_text = "["
        for _,h in pairs(hosts) do
            if common.get_host_status(h) then
                host = h
            end
            hosts_text = hosts_text .. " " .. h 
        end
        hosts_text = hosts_text .. " ]"

        if host == nil then
            ngx.log(ngx.WARN,'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment ' .. hosts_text)
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        else
            -- Redirect to one of the corrent hosts here. 307.
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object,bucket)
            ngx.header["Location"] = url
            ngx.log(ngx.INFO,'Redirecting POST request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text)
            exitcode = 307
        end
    end
    return exitcode
end

local function put_object(internal, bucket, object, req_body_file)
    local exitcode = ngx.HTTP_OK
    ngx.req.read_body()
    local req_body_file = ngx.req.get_body_file()
    return exitcode
end

local function delete_object(internal, bucket, object)
    local exitcode = ngx.HTTP_OK
    return exitcode
end

local r = common.parse_request()
--ngx.header["server"] = nil

local exitcode = nil

local method = r['method']
if method == "POST" then
    exitcode = post_object(r)
elseif method == "PUT" then
    exitcode = put_object(r['internal'], r['bucket'], r['object'])
elseif method == "DELETE" then
    exitcode = delete_object(r['internal'], r['bucket'], r['object'])
elseif method == "GET" or method == "HEAD" then
    if r['bucket'] then
        exitcode = bucket_index(r)
    end
end

local elapsed = ngx.now() - ngx.req.start_time()
if not ngx.headers_sent then
    if elapsed > 0 then
        ngx.header["x-elapsed"] = elapsed
    end
end

ngx.exit(exitcode)

