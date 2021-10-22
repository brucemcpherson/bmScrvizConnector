var Digestive = (() => {

  const DIGEST_PREFIX = '@mild@'
  const MAX_CACHE_SIZE = 100 * 1024


  const digest = (...args) => {
    // conver args to an array and digest them
    const t = args.concat([DIGEST_PREFIX]).map(d => {
      return (Object(d) === d) ? JSON.stringify(d) : (typeof d === typeof undefined ? 'undefined' : d.toString());
    }).join("-")

    const s = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_1, t, Utilities.Charset.UTF_8)
    return Utilities.base64EncodeWebSafe(s)
  };

  /**
   * zip some content - for this use case - it's for cache, we're expecting string input/output
   * @param {string} crushThis the thing to be crushed
   * @raturns {string}  the zipped contents as base64
   */
  const crush = (crushThis) => {
    return Utilities.base64Encode(Utilities.zip([Utilities.newBlob(crushThis)]).getBytes());
  }

  /**
   * unzip some content - for this use case - it's for cache, we're expecting string input/output
   * @param {string} crushed the thing to be uncrushed - this will be base64 string
   * @raturns {string}  the unzipped and decoded contents
   */
  const uncrush = (crushed) => {
    return Utilities.unzip(Utilities.newBlob(Utilities.base64Decode(crushed), 'application/zip'))[0].getDataAsString();
  }

  /**
   * gets and reconstitues cache from a series of compressed entries
   */
  const cacheGetHandler = (cacheService, ...args) => {
    // call the cache get function and make the keys
    const d = digest.apply(null, args)
    const h = cacheService.get(d)
    if (!h) return null;
    const header = JSON.parse(h)

    // we have to reconstitute all the entries
    const str = header.subs.reduce((p, c) => {
      const e = cacheService.get(c)
      // and entry has disappeared, so give up
      if (!e) return null
      return p + e
    }, '')

    return {
      ...header,
      data: JSON.parse(uncrush(str))
    }

  }

  const chunker = (str, len) => {
    const chunks = [];
    let i = 0
    const n = str.length;
    while (i < n) {
      chunks.push(str.slice(i, i += len));
    }
    return chunks;
  }

  /**
   * this will not only compress, but also spread result across multiple cache entries
   */
  const cacheSetHandler = (cacheService,...args) => {
    const [data, expiry, ...keys] = args
    const d = digest.apply(null, keys)
    const strif = JSON.stringify(data)
    const crushed = crush(strif)
    const subs = chunker(crushed, MAX_CACHE_SIZE).map((f, i) => {
      const key = digest(d, i)
      cacheService.put(key, f, expiry)
      return key
    })

    const pack = {
      timestamp: new Date().getTime(),
      digest,
      subs
    }
    // always want the header to expire before the trailers
    cacheService.put(d, JSON.stringify(pack), Math.max(0, expiry - 1))
    return pack
  }
  return {
    cacheGetHandler,
    cacheSetHandler
  }
})()
