import jp from "jsonpath";
console.log(jp.parse(process.argv.slice(2).join(" ") || ""));