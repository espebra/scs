local class = require "kidclass"
local Configuration = require "configuration"
local Request = class.new();
local ngx = ngx

---------------
-- Private API
---------------
local function get_directory_depth(md5)
    local dir = false
    if md5 then
        local m, err = ngx.re.match(md5, "^(..)(..)",'j')
        if m then
            if #m == 2 then
                dir = m[1] .. "/" .. m[2]
            end
        end
    end
    return dir
end

local function verify_bucket(bucket)
    -- Must not be false
    if not bucket then
        --ngx.log(ngx.WARN,"Bucket name is not set")
        return false
    end

    -- Must not be less than 3 characters
    if #bucket < 3 then
        --ngx.log(ngx.WARN,"Bucket name is too short")
        return false
    end

    -- Must not be more than 63 characters
    if #bucket > 63 then
        --ngx.log(ngx.WARN,"Bucket name is too long")
        return false
    end

    -- Must contain only allowed characters
    if not ngx.re.match(bucket, '^[a-z0-9-]+$','j') then
        --ngx.log(ngx.WARN,"Bucket name contains illegal characters")
        return false
    end

    -- Must not start with -
    if ngx.re.match(bucket, '^-','j') then
        --ngx.log(ngx.WARN,"Bucket name starts with -")
        return false
    end

    -- Must not end with -
    if ngx.re.match(bucket, '-$','j') then
        --ngx.log(ngx.WARN,"Bucket name ends with -")
        return false
    end

    return true
end

local function _is_internal(useragent)
    if useragent == "scs internal" then
        return true
    else
        return false
    end
end

---------------
-- Public API
---------------
function Request.Constructor(self)
    local conf = Configuration()
    self.storage = conf.storage

    local h = ngx.req.get_headers()
    local args = ngx.req.get_uri_args()

    -- Used to identify internal requests between the replica hosts. 
    -- This is not a security feature.
    if h['user-agent'] then
        self.internal = _is_internal(h['user-agent'])
    end

    -- PING request. These are used as health checks between replica hosts.
    if h['x-ping'] then
        self.ping = true
    elseif args['x-ping'] then
        self.ping = true
    end

    -- Let the client specify a version of the object to fetch
    self.version = nil
    if h['x-version'] then
        self.version = h['x-version']
    elseif args['x-version'] then
        self.version = args['x-version']
    end

    if self.version then
        if not ngx.re.match(self.version, '^[0-9]+$','j') then
            ngx.log(ngx.ERR,"Request version contains invalid characters")
            self.version = nil
        else
            -- Cast if the content is valid
            self.version = tonumber(self.version)
        end
    end

    -- Return meta data
    if h['x-meta'] then
        self.meta = true
    elseif args['x-meta'] then
        self.meta = true
    end

    if h['debug'] then
        self.debug = true
    elseif args['debug'] then
        self.debug = true
    end

    if h['x-md5'] then
        self.object_md5 = h['x-md5']

        -- Check the md5 content
        if not ngx.re.match(self.object_md5, '^[a-f0-9]+$','j') then
            ngx.log(ngx.ERR,"request md5 header contains non-valid characters")
            self.object_md5 = nil
        end
    
        -- Check the md5 length
        if not #self.object_md5 == 32 then
            ngx.log(ngx.ERR,"request md5 header length is invalid")
            self.object_md5 = nil
        end
    end

    -- The bucket is the value of the argument bucket, or the hostname in the
    -- host header.
    if args['bucket'] then
        if verify_bucket(args['bucket']) then
            self.bucket = args['bucket']
        end
    else
        local m, err = ngx.re.match(ngx.var.host, '^([^.]+)','j')
        if m then
            if #m == 1 then
                if verify_bucket(m[1]) then
                    self.bucket = m[1]
                end
            end
        end
    end

    -- Read the object name, and remove the first char (which is a /)
    local object = string.sub(ngx.var.uri, 2)

    -- Unescape the filename of the object before hashing
    self.object = ngx.unescape_uri(object)

    -- Set both the object and object_base64 to nil if the length of the object
    -- name is 0.
    if self.bucket and #self.object > 0 then
        self.object_base64 = ngx.encode_base64(self.object)
        self.object_name_md5 = ngx.md5(self.object)
        self.dir = "/" .. self.bucket .. "/" .. get_directory_depth(self.object_name_md5) .. "/" .. self.object_base64
    else
        -- Do not allow 0 character object names
        self.object = nil
        self.object_base64 = nil
        self.object_name_md5 = nil
    end

    self.method = ngx.var.request_method

    -- Clean up the headers here
    ngx.header['Server'] = 'scs'
end

return Request
