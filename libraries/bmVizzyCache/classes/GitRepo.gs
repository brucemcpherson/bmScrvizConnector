class GitRepo {



  constructor({ repository, importFields }) {
    if (importFields) {
      this.fields = importFields;
    } else {
      const { owner } = repository;
      this.fields = ["id", "full_name", "name", "html_url", "url"].reduce(
        (p, c) => {
          p[c] = repository[c];
          return p;
        },
        {}
      );
      this.fields.ownerId = owner.id;
  
    }
  }


  decorate(body) {
    if (body) {
      [].forEach((f) => {
        this.fields[f] = body[f];
      });
    }
  }
}


