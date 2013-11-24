local ngx = require "ngx"
local common = require "common"
local Request = require "request"
--local Configuration = require "configuration"

local timer = require "timer"
--local cjson = require 'cjson'

local function rewrite_request(r)
    local object = r.object
    local bucket = r.bucket
    local internal = r.internal
    local object_base64 = r.object_base64
    local dir = r.dir
    local storage = r.storage

    local version = 0
    local md5 = false

    local versions = common.get_local_object(storage .. dir)
    for _,v in ipairs(versions) do
         if r.version then
             -- The client requests a spesific version
             if r.version == v['version'] then
                  version = v['version']
                  md5 = v['md5']
                  break
             end
         else
             -- The client does not care about the version. Deliver the latest one.
             if v['version'] > version then
                  version = v['version']
                  md5 = v['md5']
             end
         end
    end

    if r.version then
        ngx.log(ngx.DEBUG, "The client requested version " .. r.version)
    else
        ngx.log(ngx.DEBUG, "The client did not specify version")
    end

    if version > 0 and md5 then
        ngx.log(ngx.DEBUG,"Will deliver version " .. version .. " with md5 " .. md5)
        ngx.header['X-Md5'] = md5
        ngx.header['X-Version'] = version
        local uri = dir .. "/" .. version .. "-" .. md5 .. ".data"
        ngx.log(ngx.DEBUG,"Found " .. bucket .. "/" .. object .. " in local file system. Rewriting URI " .. ngx.var.uri .. " to " .. uri)
        ngx.req.set_uri(uri, true)
    else
        if r.internal then
            -- If it's an internal request, send reply according to the local result only.
            return ngx.exit(ngx.HTTP_NOT_FOUND)
        end
    end

    if not r.internal then
        ngx.log(ngx.DEBUG,"Public request. Should respond with 404 or 200 according to the cluster.")
    end
    -- None found
end

--local function rewrite_request(r)
--    local object = r.object
--    local bucket = r.bucket
--    local dir = r.dir
--    local internal = r.internal
--    local object_base64 = r.object_base64
--    local dir = common.get_storage_directory()
--    local found_locally = false
--
--    if object and bucket and object_base64 then
--        -- See if the object exists locally, and rewrite if it does
--        local versions = common.get_local_object_versions(bucket, object)
--        if #versions > 0 then
--            local version = false
--            local md5 = false
--
--            if r.version then
--                ngx.log(ngx.INFO,"The request is version spesific, version " .. r.version)
--                for _,v in ipairs(versions) do
--                    if r.version == v['version'] then
--                        ngx.log(ngx.DEBUG,"Considering versions: " .. r.version .. " is " .. v['version'])
--                        version = v['version']
--                        md5 = v['md5']
--                    else
--                        ngx.log(ngx.DEBUG,"Considering versions: " .. r.version .. " is not " .. v['version'])
--                    end
--                end
--            else
--                version = versions[1].version
--                md5 = versions[1].md5
--            end
--
--            if version and md5 then
--                found_locally = true
--                -- Print the checksum and version as response headers
--                ngx.header['X-Md5'] = md5
--                ngx.header['X-Version'] = version
--                local depth = common.get_directory_depth(object)
--                local uri = "/" .. bucket .. "/" .. depth .. "/" .. object_base64 .. "/" .. version .. "-" .. md5 .. ".data"
--                ngx.log(ngx.INFO,"Found " .. bucket .. "/" .. object .. " in local file system. Rewriting URI " .. ngx.var.uri .. " to " .. uri)
--                ngx.req.set_uri(uri, true)
--            end
--        end
--    end
--
--    -- None found
--end

---- Read the configuration
--local conf = Configuration()

---- Read the request
local r = Request()

---- Start periodic batch jobs here
timer.initiate_periodic_health_checks(10)

---- Return 200 to the ping check
if r.ping and r.internal then
    ngx.log(ngx.DEBUG,"Received ping request from " .. ngx.var.remote_addr)
    return ngx.exit(ngx.HTTP_OK)
end

-- Only if a GET or HEAD to a specific object that may exist locally
if (r.method == "GET" or r.method == "HEAD") and r.object and r.bucket and not r.meta then
    rewrite_request(r)
end

--for host, v in pairs(conf.hosts) do
--    --ngx.log(ngx.INFO,"Caching: Host " .. host)
--end
