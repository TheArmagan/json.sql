function getPathCombinations(obj: any, prefix: any[] = []): any[] {
  let paths: any[] = [];

  if (typeof obj === "object" && obj !== null) {
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        paths = paths.concat(getPathCombinations(obj[i], prefix.concat(i)));
      }
    } else {
      for (const key in obj) {
        if (key in obj) {
          paths = paths.concat(getPathCombinations(obj[key], prefix.concat(key)));
        }
      }
    }
  } else {
    paths.push([prefix, obj]);
  }

  return paths;
}

console.log(getPathCombinations("hello"));