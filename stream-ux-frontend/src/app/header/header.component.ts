import { Component, Input, OnInit } from '@angular/core';
import { faBars, faSearch, faAngleDown } from '@fortawesome/free-solid-svg-icons';


@Component({
  selector: 'app-header',
  styleUrls: ['./header.component.scss'],
  templateUrl: './header.component.html'
})
export class HeaderComponent implements OnInit {
  searchQuery: string;
  cimsUrl: string;
  searchToggled = false;
  isCollapsed = true;
  isToggled = true;
  icon = { faBars, faSearch, faAngleDown };

  constructor() { }

  ngOnInit() { }

  toggleSearch() {
    this.searchToggled = !this.searchToggled;
  }

  onSearchKeyPress(event) {
    if (event.keyCode === 13) {
      this.doSearch();
    }
  }

  onSearchClick(event) {
    event.preventDefault();
    this.doSearch();
  }

  doSearch() {
    if (window && this.searchQuery) {
      window.location.href =
        'http://www.gms.ca/search?Text=' + this.searchQuery;
    }
  }

  isLoggedIn() {
    return false;
  }

  displayName() { }

  logout() { }

  toggleDropdown() {
    this.isToggled = !this.isToggled;
  }
}
