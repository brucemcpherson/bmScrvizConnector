

const flattenVizzyOwners = (data) => {

  const { owners, repos, files, libraries } = data

  const result = owners.map(({ fields }) => {
    const { id } = fields
    const ownedFiles = files.filter(file => file.fields.ownerId === id)
    const ownedRepos = repos.filter(repo => repo.fields.ownerId === id)
    const ownedClaspFiles = ownedFiles.filter(file => file.fields.claspHtmlUrl)
    const ownedLibraries = libraries.filter(library => library.ownerId === id)

    return {
      ownerName: fields.name,
      ownerLocation: fields.location,
      ownerHireable: fields.hireable,
      ownerPublicRepos: fields.public_repos,
      ownerFollowers: fields.followers,
      ownerId: id,
      ownerAppsScriptRepos: ownedRepos.length,
      ownerTwitter: fields.twitter_userName,
      ownerEmail: fields.email,
      ownerGithub: fields.login,
      ownerBlog: fields.blog,
      ownerProjects: ownedFiles.length,
      ownerLibraries: ownedLibraries.length,
      ownerLibraryReferences: ownedLibraries.reduce((p, c) => p + c.referencedBy.length, 0),
      ownerClaspProjects: ownedClaspFiles.length,
      ownerLibraryDependencies: libraries.reduce((p,c)=>{
        return c.referencedBy.reduce((xp,xc)=> ownedFiles.filter(g=>g.fields.sha===xc.sha).length+p ,p)
      },0)
    }
  })
  // unknown libraries where the library hasnt been found on scrviz
  
  const unknownLibraries = libraries.map((f,i)=>({
    ...f,
    index:i
  })).filter(f=>!f.ownerId)

  return {
    result: result.map(f=>{
      f.ownerLibrariesUnknown = unknownLibraries.length
      return f
    }),
    unknownLibraries
  }
}



