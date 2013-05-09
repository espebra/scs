local common = require "scs.common"
local timer = require "scs.timer"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

local function head_object(internal, bucket, object)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg = nil

    local object_base64 = ngx.encode_base64(object)
    local dir = common.get_storage_directory()

    -- See if the object exists locally
    if common.object_exists_locally(dir, bucket, object_base64) then
        exitcode = ngx.HTTP_OK
        msg = "The object " .. object .. " in bucket " .. bucket .. " exists locally."
    else
        if not internal then
            -- Redirect to another host if this is not an internal request
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
                local url = common.generate_url(host,port,object)
                msg = 'Redirecting HEAD request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
                ngx.header["Location"] = url
                exitcode = ngx.HTTP_MOVED_TEMPORARILY
            end
        end
    end
    return exitcode, msg
end

local function get_object(internal, bucket, object)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg = nil
    -- See if the object exists locally
    local object_base64 = ngx.encode_base64(object)
    local dir = common.get_storage_directory()
    if common.object_exists_locally(dir, bucket, object_base64) then
        -- We have the file locally. Serve it directly. 200.
        ngx.header["content-disposition"] = "attachment; filename=" .. object;
        local path = dir .. "/" ..  bucket
        local fp = io.open(path .. "/" .. object_base64, 'r')
        local size = 2^20      -- good buffer size (1M)
        -- Stream the contents of the file to the client
        while true do
            local block = fp:read(size)
            if not block then break end
            ngx.print(block)
        end
        fp:close()
        local msg = "Object " .. object .. " in bucket " .. bucket .. " delivered successfully to the client."
        exitcode = ngx.HTTP_OK
    else
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
                local url = common.generate_url(host,port,object)
                -- ngx.say("Host: " .. host)
                -- ngx.say("Redirect to: " .. url)
                msg = 'Redirecting GET request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
                ngx.header["Location"] = url
                exitcode = ngx.HTTP_MOVED_TEMPORARILY
            end
        end
    end
    return exitcode, msg
end

local function post_object(internal, bucket, object)
    local sites = common.get_object_replica_sites(bucket, object)
    local hosts = common.get_replica_hosts(bucket, object, sites)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg = nil

    local dir = common.get_storage_directory()
    if common.object_fits_on_this_host(hosts) then
        local object_base64 = ngx.encode_base64(object)
        local path = dir .. "/" ..  bucket
        if not os.rename(path, path) then
            os.execute('mkdir -p ' .. path)
        end

        ngx.req.read_body()
        local req_body_file = ngx.req.get_body_file()

        if not req_body_file then
            msg = 'No file found in request'
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        if req_body_file == nil then
            msg = 'Request body is nil'
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

        if common.object_exists_locally(dir, bucket, object_base64) then
            msg = 'The object ' .. object .. ' in bucket ' .. bucket .. ' was written successfully to local file system.'
            exitcode = ngx.HTTP_OK
        else
            msg = 'Failed to write object ' .. object .. ' in bucket ' .. bucket .. ' to local file system'
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
                local res = common.sync_object(dir, host, bucket, object_base64)
                if res then
                    ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " succeeded.")
                else
                    ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " failed.")
                end
            else
                ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " not initiated. The host is down.")
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
            msg = 'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment ' .. hosts_text
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        else
            -- Redirect to one of the corrent hosts here. 307.
            --ngx.log(ngx.ERR,"Found " .. #hosts .. " available hosts, selected " .. host)
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object)
            ngx.header["Location"] = url
            msg = 'Redirecting POST request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
            exitcode = 307
        end
    end
    return exitcode, msg
end

local function put_object(internal, bucket, object, req_body_file)
    local msg
    local exitcode = ngx.HTTP_OK
    ngx.req.read_body()
    local req_body_file = ngx.req.get_body_file()
    return exitcode, msg
end

local function delete_object(internal, bucket, object)
    local msg
    local exitcode = ngx.HTTP_OK
    return exitcode, msg
end

timer.initiate_periodic_health_checks(5)

ngx.header["server"] = nil
local h = ngx.req.get_headers()
local internal = common.is_internal_request(h['user-agent'])
local debug = h['x-debug']
local status = h['x-status']
local bucket = h['x-bucket']

local exitcode = nil
local msg = nil

-- Return 200 immediately if the x-status header is set. This is to verify that
-- the host is up and running.
if status and internal then
    exitcode = ngx.HTTP_OK
    msg = "Returning 200 to the status check."
end

if not status and not bucket then
    exitcode = ngx.HTTP_BAD_REQUEST
    msg = "The request is missing the x-bucket header."
end

if not status and not common.verify_bucket(bucket) then
    exitcode = ngx.HTTP_BAD_REQUEST
    if bucket == nil then
        msg = "Invalid bucket name."
    else
        msg = "Invalid bucket name (" .. bucket .. ")."
    end
end

-- Read the object name, and remove the first char (which is a /)
local object = string.sub(ngx.var.request_uri, 2)
if not status and string.len(object) == 0 then
    exitcode = ngx.HTTP_BAD_REQUEST
    msg = "The object name is not set."
end

-- Unescape the filename of the object before hashing
object = ngx.unescape_uri(object)

-- Assemble the request metadata table
local r = {
  ['object'] = object, -- Plain text name of the object
  ['object_base64'] = ngx.encode_base64(object), -- Base64 name of the object
  ['method'] = ngx.var.request_method, -- Request method (HEAD, GET, POST, ..)
  ['bucket'] = bucket, -- Name of the bucket
  ['internal'] = internal, -- True if internal signaling request
  ['debug'] = debug, -- Add debug information in the response
}

--exitcode, msg = route.request(r)
if not exitcode then
    local method = r['method']
    if method == 'HEAD' then
        exitcode, msg = head_object(internal, bucket, object)
    elseif method == "GET" then
        exitcode, msg = get_object(internal, bucket, object)
    elseif method == "POST" then
        exitcode, msg = post_object(internal, bucket, object)
    elseif method == "PUT" then
        exitcode, msg = put_object(internal, bucket, object)
    elseif method == "DELETE" then
        exitcode, msg = delete_object(internal, bucket, object)
    end
end

local elapsed = ngx.now() - ngx.req.start_time()
if not ngx.headers_sent then
    if elapsed > 0 then
        ngx.header["x-elapsed"] = elapsed
    end
end

if debug then
    if msg then
        ngx.log(ngx.ERR, "Req time: " .. elapsed .. " sec. " .. msg)
    else
        ngx.log(ngx.ERR, "Req time: " .. elapsed .. " sec. No message")
    end
end
ngx.exit(exitcode)
