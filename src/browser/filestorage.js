import { dbg_assert } from "../log.js";
import { load_file } from "../lib.js";

/** @interface */
export function FileStorageInterface() {}

/**
 * Read a portion of a file.
 * @param {string} sha256sum
 * @param {number} offset
 * @param {number} count
 * @param {number} file_size
 * @return {!Promise<Uint8Array>} null if file does not exist.
 */
FileStorageInterface.prototype.read = function(sha256sum, offset, count, file_size) {};

/**
 * Add a read-only file to the filestorage.
 * @param {string} sha256sum
 * @param {!Uint8Array} data
 * @return {!Promise}
 */
FileStorageInterface.prototype.cache = function(sha256sum, data) {};

/**
 * Call this when the file won't be used soon, e.g. when a file closes or when this immutable
 * version is already out of date. It is used to help prevent accumulation of unused files in
 * memory in the long run for some FileStorage mediums.
 */
FileStorageInterface.prototype.uncache = function(sha256sum) {};

/**
 * @constructor
 * @implements {FileStorageInterface}
 */
export function MemoryFileStorage()
{
    /**
     * From sha256sum to file data.
     * @type {Map<string,Uint8Array>}
     */
    this.filedata = new Map();
}

/**
 * @param {string} sha256sum
 * @param {number} offset
 * @param {number} count
 * @return {!Promise<Uint8Array>} null if file does not exist.
 */
MemoryFileStorage.prototype.read = async function(sha256sum, offset, count)
{
    dbg_assert(sha256sum, "MemoryFileStorage read: sha256sum should be a non-empty string");
    const data = this.filedata.get(sha256sum);

    if(!data)
    {
        return null;
    }

    return data.subarray(offset, offset + count);
};

/**
 * @param {string} sha256sum
 * @param {!Uint8Array} data
 */
MemoryFileStorage.prototype.cache = async function(sha256sum, data)
{
    dbg_assert(sha256sum, "MemoryFileStorage cache: sha256sum should be a non-empty string");
    this.filedata.set(sha256sum, data);
};

/**
 * @param {string} sha256sum
 */
MemoryFileStorage.prototype.uncache = function(sha256sum)
{
    this.filedata.delete(sha256sum);
};

/**
 * @constructor
 * @implements {FileStorageInterface}
 * @param {FileStorageInterface} file_storage
 * @param {string} baseurl
 * @param {function(number,Uint8Array):ArrayBuffer} zstd_decompress
 */
export function ServerFileStorageWrapper(file_storage, baseurl, zstd_decompress)
{
    dbg_assert(baseurl, "ServerMemoryFileStorage: baseurl should not be empty");

    if(!baseurl.endsWith("/"))
    {
        baseurl += "/";
    }

    this.storage = file_storage;
    this.baseurl = baseurl;
    this.zstd_decompress = zstd_decompress;
    this.split_manifest = undefined;
    this.split_manifest_promise = null;
}

/**
 * Fetch a small JSON file without the retry loop used for large images.
 * This lets us treat a missing split manifest as optional instead of hanging forever.
 * @param {string} url
 * @return {!Promise<ArrayBuffer>}
 */
ServerFileStorageWrapper.prototype.fetch_array_buffer = function(url)
{
    return new Promise((resolve, reject) =>
    {
        const http = new XMLHttpRequest();
        http.open("get", url, true);
        http.responseType = "arraybuffer";

        http.onload = function()
        {
            if(http.status === 200 || http.status === 206)
            {
                resolve(http.response);
            }
            else
            {
                reject(new Error("Loading " + url + " failed (status " + http.status + ")"));
            }
        };

        http.onerror = function(e)
        {
            reject(e);
        };

        http.send(null);
    });
};

/**
 * @return {!Promise<!Object<string, !Array<string>>>}
 */
ServerFileStorageWrapper.prototype.get_split_manifest = function()
{
    if(this.split_manifest !== undefined)
    {
        return Promise.resolve(this.split_manifest);
    }

    if(this.split_manifest_promise)
    {
        return this.split_manifest_promise;
    }

    this.split_manifest_promise = this.fetch_array_buffer(this.baseurl + "split-manifest.json")
        .then(buffer =>
        {
            const text = new TextDecoder().decode(new Uint8Array(buffer));
            const manifest = JSON.parse(text);
            this.split_manifest = manifest && typeof manifest === "object" ? manifest : {};
            return this.split_manifest;
        })
        .catch(() =>
        {
            this.split_manifest = {};
            return this.split_manifest;
        });

    return this.split_manifest_promise;
};

/**
 * @param {!Array<string>} parts
 * @return {!Promise<Uint8Array>}
 */
ServerFileStorageWrapper.prototype.load_split_file = async function(parts)
{
    const buffers = await Promise.all(parts.map(async part =>
    {
        const buffer = await this.fetch_array_buffer(this.baseurl + part);
        return new Uint8Array(buffer);
    }));

    let total_length = 0;
    for(const buffer of buffers)
    {
        total_length += buffer.length;
    }

    const combined = new Uint8Array(total_length);
    let offset = 0;

    for(const buffer of buffers)
    {
        combined.set(buffer, offset);
        offset += buffer.length;
    }

    return combined;
};

/**
 * @param {string} sha256sum
 * @param {number} file_size
 * @return {!Promise<Uint8Array>}
 */
ServerFileStorageWrapper.prototype.load_from_server = async function(sha256sum, file_size)
{
    const split_manifest = await this.get_split_manifest();
    const parts = split_manifest[sha256sum];

    if(parts)
    {
        let data = await this.load_split_file(parts);

        if(sha256sum.endsWith(".zst"))
        {
            data = new Uint8Array(
                this.zstd_decompress(file_size, data)
            );
        }

        await this.cache(sha256sum, data);
        return data;
    }

    return new Promise((resolve, reject) =>
    {
        load_file(this.baseurl + sha256sum, { done: async buffer =>
        {
            let data = new Uint8Array(buffer);
            if(sha256sum.endsWith(".zst"))
            {
                data = new Uint8Array(
                    this.zstd_decompress(file_size, data)
                );
            }
            await this.cache(sha256sum, data);
            resolve(data);
        }});
    });
};

/**
 * @param {string} sha256sum
 * @param {number} offset
 * @param {number} count
 * @param {number} file_size
 * @return {!Promise<Uint8Array>}
 */
ServerFileStorageWrapper.prototype.read = async function(sha256sum, offset, count, file_size)
{
    const data = await this.storage.read(sha256sum, offset, count, file_size);
    if(!data)
    {
        const full_file = await this.load_from_server(sha256sum, file_size);
        return full_file.subarray(offset, offset + count);
    }
    return data;
};

/**
 * @param {string} sha256sum
 * @param {!Uint8Array} data
 */
ServerFileStorageWrapper.prototype.cache = async function(sha256sum, data)
{
    return await this.storage.cache(sha256sum, data);
};

/**
 * @param {string} sha256sum
 */
ServerFileStorageWrapper.prototype.uncache = function(sha256sum)
{
    this.storage.uncache(sha256sum);
};
