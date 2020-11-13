import { Component, Input, OnInit } from '@angular/core';

@Component({
  selector: 'breadcrumbs',
  templateUrl: './breadcrumbs.component.html',
  styleUrls: ['./breadcrumbs.component.css']
})
export class BreadcrumbsComponent implements OnInit {

  private _path: Array<string> = [];

  @Input('path') set path(path: Array<string>) {
    if (path.length === 0) {
      path = ['Home'];
    }
    this._path = path;
  }

  get path(): Array<string> {
    return this._path;
  }

  constructor() {
  }

  ngOnInit(): void {
  }
}
