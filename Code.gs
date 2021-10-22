// https://datastudio.google.com/datasources/create?connectorId=AKfycbxeUUmp9_nReUHKW92y77nW3INaWmloIfxo6MfwPh0

// generic test to see if we're allowed to use cache
const _cacheService = CacheService.getScriptCache();

// This namespace is all about getting and formatting data
const dataManip = (() => {

  // we should cache as there will be lots of accesses when setting up datastudio report
  // and scrviz doesn't run very often

  const EXPIRE = 3000
  const CACHE_KEYS = ['bmScrviz', 'items']
  const ITEMS = ['types', 'owners', 'repos', 'shaxs', 'files']
  const CACHE_STUDIO_KEYS = ['bmScrviz', 'studio']
  const MANIFEST_ITEMS = ['libraries', 'timeZones', 'webApps', 'runtimeVersions', 'addOns', 'oauthScopes', 'dataStudios']
  const EXPIRE_STUDIO = 100
  const _fromCache = (request) => !(!request || !_cacheService || (request.scriptParams && request.scriptParams.noCache))

  /**
   * cache handling/crushing etc is all delegated to Digestive namespace
   */
  const cacheGetter = () => Digestive.cacheGetHandler(_cacheService, CACHE_KEYS)
  const cacheSetter = (data) => Digestive.cacheSetHandler(_cacheService, data, EXPIRE, CACHE_KEYS)
  const cacheStudioGetter = (request) => Digestive.cacheGetHandler(_cacheService, CACHE_STUDIO_KEYS, request)
  const cacheStudioSetter = (data, request) => Digestive.cacheSetHandler(_cacheService, data, EXPIRE_STUDIO, CACHE_STUDIO_KEYS, request)

  const _compareLabels = (a,b) => {
    // ingnore case/
    const alab = a.toLowerCase();
    const blab = b.toLowerCase();
    return alab === blab ? 0 : (alab > blab ? 1 : -1)
  }

  const _compare = (a, b) => _compareLabels (a.label, b.label)

  const _looserCompare = (a,b) => (a,b) => {
    // ingnore case and -/
    const alab = a.toLowerCase().replaceAll('-','');
    const blab = b.toLowerCase().replaceAll('-','');
    return alab === blab ? 0 : (alab > blab ? 1 : -1)
  }

 
  /**
   * try to sort out the libraries
   */
  const sortOutLibraries = (data) => {

    // we need to optimize mapping shaxs to files to do this only once
    const msf = new Map (data.shaxs.map(f=>[
      f.fields.sha,
      data.files.filter(g=>f.fields.sha === g.fields.sha)
    ]))

    // we also need to know which shaxs have lib dependencies multiple times
    const s =  new Map (data.shaxs.map(f=>[
      f.fields.sha,
      f.fields.content && 
      f.fields.content.dependencies && 
      f.fields.content.dependencies.libraries && 
      f.fields.content.dependencies.libraries.map(g=> g.libraryId)
    ]).filter(([k,v])=>v && v.length))

    // ssf is a map shaxs which reference a given libraryID
    const ssf = Array.from(s).reduce((p,[k,v])=> {
      v.forEach(g=>{
        if(!p.has(g)) p.set(g,[])
        p.get(g).push(k)
      })
      return p
    }, new Map()) 

    // special clues from those with multiple projects in a repo
    const mReps = data.repos.map(g=>({
      repo: g,
      multiples:data.files.filter(h=>h.fields.repositoryId === g.fields.id).map(h=> ({
        repo: h,
        projectName: h.fields.path
          .replace('src/appscript.json','appsscript.json')
          .replace('dist/appscript.json','appsscript.json')
          .replace(/.*\/(.*)\/appsscript.json$/,"$1")
      }))
    })).filter(g=>g.multiples.length>1)

    // now we look at all the known libraries
    // libraries only have an id a list of versions in use, and a label
    // we have to try to see if we somehow match then up to known files
    // however we don't have a scriptID for each file
    return data.libraries.sort(_compare)

      .map(f => {

        const file = data.files.find(g=>f.id === g.fields.scriptId)

        // otherwise its all a bit flaky
        let repo = data.repos.find(g => file && g.fields.id ===  file.fields.repositoryId)
        const owner = repo && data.owners.find(g => g.fields.id === repo.fields.ownerId)
        const referencedBy = ssf.get(f.id)
        const ob = {
          ...f,
          repoId: repo && repo.fields.id,
          ownerId: owner && owner.fields.id,
          repo: repo && repo.fields.name,
          repoLink: repo && repo.fields.html_url,
          owner: owner && owner.fields.name,
          claspProject: (file && file.fields.claspHtmlUrl && file.fields.claspHtmlUrl.replace('/.clasp.json', '')) || false,
          referencedBy
        }
        return ob
      })
  }

  /**
   * gets the stats from the scrviz repo
   */
  const getVizzy = (request) => {

    // whether to cache is passed in the request from datastudio
    const c = _fromCache(request) && cacheGetter()

    if (c) {
      console.log('Scrviz data was from cache ', new Date().getTime() - c.timestamp)
      return c.data
    } else {
      const { gd, mf } = bmVizzyCache.VizzyCache.fetch(UrlFetchApp.fetch)
      const data = ITEMS.reduce((p, c) => {
        p[c] = gd.items(c)
        return p
      }, {})

      MANIFEST_ITEMS.reduce((p, c) => {
        if (mf._maps[c]) p[c] = Array.from(mf._maps[c].values())
        return p
      }, data)

      // now let's see if we can find the libraries referred to 
      data.libraries = (data.libraries && sortOutLibraries(data)) || []
      cacheSetter(data)
      return data
    }
  }

  /**
   * Gets response for UrlFetchApp.
   *
   * @param {Object} request Data request parameters.
   * @returns {object} Response from vizzycache library
   */
  const fetchDataFromApi = (request) => {
    return getVizzy(request)
  };

  // selects all the fields required for the connector
  const normalizeResponse = (data) => flattenVizzyOwners(data)

  // formats the selected fields
  const getFormattedData = (response, requestedFields, schema) =>
    response.map(item => formatData(requestedFields, item, schema))



  /**
   * Formats a single row of data into the required format.
   *
   * @param {Object} requestedFields Fields requested in the getData request.
   * @param {Object} item 
   * @returns {Object} Contains values for requested fields in predefined format.
   */
  const formatData = (requestedFields, item, schema) => {

    var row = requestedFields.asArray().map((requestedField, i) => {
      const v = item[requestedField.getId()]

      // no formatting required, except to clean up nulls/udefined in boolean values
      switch (schema[i].dataType) {
        case "BOOLEAN":
          return Boolean(v)
        case "STRING":
          return v === null || typeof v === typeof undefined ? '' : v.toString()
        default:
          return v
      }
    })
    return { values: row };
  };

  return {
    /**
     * fetchit just combines the gettinf and formatting of datastudio response
     */
    fetchIt: (request, requestedFields, schema) => {
      const apiResponse = fetchDataFromApi(request);
      const normalizedResponse = normalizeResponse(apiResponse);
      return getFormattedData(normalizedResponse.result, requestedFields, schema);
    },
    getVizzy,
    cacheStudioSetter,
    cacheStudioGetter
  };
})();

// this namespace defines and exports all the required methods for a datastudio connector
var Connector = (() => {
  const { fetchIt, getVizzy, cacheStudioSetter, cacheStudioGetter } = dataManip;
  const cc = DataStudioApp.createCommunityConnector();
  const _fromCacheStudio = (request) => !(!request || !_cacheService || (request.scriptParams && request.scriptParams.noCacheStudio))

  const getConfig = () => {
    const config = cc.getConfig();

    config
      .newCheckbox()
      .setId('noCacheStudio')
      .setName('disable formatted data caching')
      .setHelpText('Data may already be available from recently run report')

    
    config
      .newCheckbox()
      .setId('noCache')
      .setName('disable catalog caching')
      .setHelpText('Data may be available from recently used scrviz access')

    return config.build();
  };

  const getFields = () => {
    var fields = cc.getFields();
    var types = cc.FieldType;
    var aggregations = cc.AggregationType;

    fields
      .newDimension()
      .setId("ownerName")
      .setName("Developer")
      .setType(types.TEXT);

    fields
      .newDimension()
      .setId("ownerHireable")
      .setName("Hireable")
      .setType(types.BOOLEAN);

    fields
      .newDimension()
      .setId("ownerLocation")
      .setName("Location")
      .setType(types.TEXT);

    fields
      .newDimension()
      .setId("ownerId")
      .setName("Owner Id")
      .setType(types.NUMBER);

    fields
      .newMetric()
      .setId("ownerFollowers")
      .setName("Followers")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);

    fields
      .newMetric()
      .setId("ownerLibraries")
      .setName("Libraries")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);

    fields
      .newMetric()
      .setId("ownerLibraryReferences")
      .setName("All References")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);

    fields
      .newMetric()
      .setId("ownerLibraryDependencies")
      .setName("Library dependencies")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);


    fields
      .newMetric()
      .setId("ownerProjects")
      .setName("Projects")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);

    fields
      .newMetric()
      .setId("ownerAppsScriptRepos")
      .setName("GAS repos")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);

    fields
      .newMetric()
      .setId("ownerPublicRepos")
      .setName("Public repos")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);

    fields
      .newMetric()
      .setId("ownerClaspProjects")
      .setName("Clasp projects")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);
    
    fields
      .newMetric()
      .setId("ownerLibrariesUnknown")
      .setName("Libraries not on github")
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);


    fields
      .newDimension()
      .setId("ownerTwitter")
      .setName("Twitter handle")
      .setType(types.TEXT);

    fields
      .newDimension()
      .setId("ownerEmail")
      .setName("Email")
      .setType(types.TEXT);

    fields
      .newDimension()
      .setId("ownerGithub")
      .setName("Github handle")
      .setType(types.TEXT);

    fields
      .newDimension()
      .setId("ownerBlog")
      .setName("Blog")
      .setType(types.TEXT);




    return fields;
  };


  const getData = (request) => {

    // whether to cache is passed in the request from datastudio
    const c = _fromCacheStudio(request) && cacheStudioGetter(request)
    if (c) {
      console.log('Studio data was from cache ', new Date().getTime() - c.timestamp)
      return c.data
    }

    // need to calculate it all
    const requestedFields = getFields().forIds(
      request.fields.map(field => {
        return field.name
      })
    );

    try {
      const schema = requestedFields.build()
      const data = fetchIt(request, requestedFields, schema);
      const response = {
        schema,
        rows: data,
      };
      cacheStudioSetter(response, request)
      return response
    } catch (e) {
      console.log(e)
      cc.newUserError()
        .setDebugText("Error fetching data from API. Exception details: " + e)
        .setText(
          "The connector has encountered an unrecoverable error. Please try again later, or file an issue if this error persists."
        )
        .throwException();
    }
  };

  // these are called by datastudio
  return {
    // https://developers.google.com/datastudio/connector/reference#getdata
    getData,

    // https://developers.google.com/datastudio/connector/reference#getconfig
    getConfig,

    // https://developers.google.com/datastudio/connector/reference#getauthtype
    getAuthType: () =>
      cc.newAuthTypeResponse().setAuthType(cc.AuthType.NONE).build(),

    // https://developers.google.com/datastudio/connector/reference#getschema
    getSchema: () => ({ schema: getFields().build() }),

    // https://developers.google.com/datastudio/connector/reference#isadminuser
    isAdminUser: () => true,

    getVizzy
  };
})();

// export these globally so that datastidio can see them
var getConfig = () => Connector.getConfig(),
  isAdminUser = () => Connector.isAdminUser(),
  getSchema = () => Connector.getSchema(),
  getAuthType = () => Connector.getAuthType(),
  getData = (request) => Connector.getData(request),
  getVizzy = () => Connector.getVizzy()


